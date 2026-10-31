"""
server.py
Run:  python server.py        then open http://127.0.0.1:8000

Starts the Angel One stream for one underlying + expiry, keeps the chain in a
MarketState, and pushes a JSON snapshot to every browser once a second.
The UI can switch underlying/expiry at runtime (POST /api/select).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import queue
import threading
import time
from contextlib import asynccontextmanager
from datetime import date, datetime
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:  # .env support is optional
    pass

import uvicorn
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from pydantic import BaseModel

from angel_core import (
    CACHE_DIR, MAX_TOKENS, MODE_FULL, MODE_QUOTE, NFO, NSE_CM, QUEUE_MAXSIZE,
    AngelSession, StreamManager, TickConsumer, cap_instruments, get_option_tokens,
    get_spot_instrument, list_underlyings, load_scrip_master, market_open_now,
    normalize_expiry, upcoming_expiries,
)
from market_state import MarketState
from paper import PaperBook, expired
from notify import Notifier
from stops import StopRules

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(threadName)-10s | %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("websocket").setLevel(logging.WARNING)
log = logging.getLogger("server")

BASE = Path(__file__).parent
DEFAULT_UNDERLYING = os.getenv("DEFAULT_UNDERLYING", "NIFTY").upper()


# --------------------------------------------------------------------------- #
# Tracker: owns session, stream, consumer and the current MarketState
# --------------------------------------------------------------------------- #
class Tracker:
    def __init__(self):
        self._lock = threading.Lock()
        self._sel_counter = 0
        self.session: AngelSession | None = None
        self.state: MarketState | None = None
        self.manager: StreamManager | None = None
        self.consumer: TickConsumer | None = None
        self._sup: threading.Thread | None = None
        self._tps_at, self._tps_n, self.tps = time.time(), 0, 0.0
        self.book = PaperBook(Path(CACHE_DIR) / "paper_book.json")
        self.notifier = Notifier()
        self.stops = StopRules(Path(CACHE_DIR) / "stop_rules.json", self.book,
                               on_alert=self.notifier.send)
        self.notifier.on_fail = lambda ch, detail: self.stops._alert(
            "system", f"{ch} message could not be delivered: {detail}")
        self._paper_at = 0.0

    def select(self, underlying: str, expiry) -> dict:
        underlying = underlying.strip().upper()
        expiry = normalize_expiry(expiry)

        # Validate first: a bad request must not tear down the running stream.
        instruments = get_option_tokens(underlying, expiry)
        spot_inst = get_spot_instrument(underlying)
        instruments = cap_instruments(instruments, limit=MAX_TOKENS - 5)

        with self._lock:
            self._stop_locked()
            if self.session is None:
                self.session = AngelSession.from_env()
            self._sel_counter += 1

            state = MarketState(self._sel_counter, underlying, expiry,
                                instruments, spot_inst, cache_dir=CACHE_DIR)
            q: queue.Queue = queue.Queue(maxsize=QUEUE_MAXSIZE)
            consumer = TickConsumer(q, state.update)

            subs = []
            if spot_inst:
                subs.append((MODE_QUOTE, [{"exchangeType": NSE_CM, "tokens": [spot_inst["token"]]}]))
            subs.append((MODE_FULL, [{"exchangeType": NFO, "tokens": [i["token"] for i in instruments]}]))
            manager = StreamManager(self.session, subs, q)

            self.state, self.consumer, self.manager = state, consumer, manager
            self._tps_at, self._tps_n, self.tps = time.time(), 0, 0.0
            consumer.start()
            self._sup = threading.Thread(target=manager.run_forever, name="supervisor", daemon=True)
            self._sup.start()

        log.info("Tracking %s %s (%d tokens, spot=%s)", underlying, expiry,
                 len(instruments), spot_inst["symbol"] if spot_inst else "none")
        return {"underlying": underlying, "expiry": expiry, "tokens": len(instruments)}

    def _stop_locked(self):
        if self.manager:
            self.manager.shutdown()
        if self._sup and self._sup.is_alive():
            self._sup.join(timeout=10)
        if self.consumer:
            self.consumer.stop()
        self.manager = self.consumer = self._sup = None

    def shutdown(self):
        with self._lock:
            self._stop_locked()
        if self.session:
            self.session.logout()

    def tick(self):
        """Called once a second from the event loop."""
        if self.state:
            self.state.sample_if_due()
            active = market_open_now() or os.getenv("STOP_RULES_ALWAYS") == "1"
            try:
                self.stops.evaluate(self.state, active)
            except Exception:  # noqa: BLE001 - a rule bug must not stop the feed loop
                log.exception("Stop-loss evaluation failed")
            if time.time() - self._paper_at >= 10:
                self._paper_at = time.time()
                self.book.record(self.state)
        if self.consumer:
            now = time.time()
            if now - self._tps_at >= 2:
                n = self.consumer.count
                self.tps = (n - self._tps_n) / (now - self._tps_at)
                self._tps_at, self._tps_n = now, n

    def snapshot(self):
        st, mgr = self.state, self.manager
        if st is None:
            return None
        snap = st.snapshot()
        snap["feed"] = {
            "status": mgr.status if mgr else "stopped",
            "error": mgr.last_error if mgr else None,
            "age": snap.pop("tick_age"),
            "tps": round(self.tps, 1),
            "market_open": market_open_now(),
        }
        snap["paper"] = self.book.view(st)
        snap["stops"] = self.stops.view(st)
        snap["alerts"] = self.stops.alert_list()
        series = self.book.pnl_series
        snap["paper_point"] = list(series[-1]) if series else None
        return snap

    def hello(self):
        h = self.state.hello()
        h["paper_series"] = list(self.book.pnl_series)
        return h


tracker = Tracker()


class Hub:
    def __init__(self):
        self.clients: dict[WebSocket, int] = {}   # ws -> sel_id it last received a hello for


hub = Hub()


def _dumps(obj) -> str:
    return json.dumps(obj, separators=(",", ":"))


async def broadcaster():
    while True:
        await asyncio.sleep(1.0)
        try:
            tracker.tick()
            if not hub.clients:
                continue
            snap = tracker.snapshot()
            if snap is None:
                continue
            payload = _dumps(snap)
            dead = []
            for ws, sid in list(hub.clients.items()):
                try:
                    if sid != snap["sel_id"]:
                        hello = tracker.hello()
                        await ws.send_text(_dumps(hello))
                        hub.clients[ws] = hello["sel_id"]
                    await ws.send_text(payload)
                except Exception:  # noqa: BLE001 - client went away
                    dead.append(ws)
            for ws in dead:
                hub.clients.pop(ws, None)
        except Exception:  # noqa: BLE001 - never let the loop die
            log.exception("Broadcast loop error")


# --------------------------------------------------------------------------- #
# App
# --------------------------------------------------------------------------- #
@asynccontextmanager
async def lifespan(app: FastAPI):
    await asyncio.to_thread(load_scrip_master)
    try:
        exps = upcoming_expiries(DEFAULT_UNDERLYING)
        if not exps:
            raise LookupError(f"No upcoming expiries for {DEFAULT_UNDERLYING}")
        await asyncio.to_thread(tracker.select, DEFAULT_UNDERLYING, exps[0])
    except Exception:  # noqa: BLE001
        log.exception("Could not start default tracking; pick an underlying in the UI")
    task = asyncio.create_task(broadcaster())
    yield
    task.cancel()
    await asyncio.to_thread(tracker.shutdown)


app = FastAPI(title="OI Ladder", lifespan=lifespan)


class SelectBody(BaseModel):
    underlying: str
    expiry: str


@app.get("/")
def index():
    return FileResponse(BASE / "static" / "index.html", headers={"Cache-Control": "no-store"})


@app.get("/api/underlyings")
def api_underlyings():
    indices, stocks = list_underlyings()
    return {"indices": indices, "stocks": stocks}


@app.get("/api/expiries")
def api_expiries(underlying: str):
    exps = upcoming_expiries(underlying)
    if not exps:
        raise HTTPException(404, f"No upcoming expiries found for '{underlying.upper()}'. "
                                 "Use the exact F&O symbol, e.g. NIFTY, BANKNIFTY, RELIANCE.")
    out = []
    for e in exps:
        d = datetime.strptime(e, "%d%b%Y")
        out.append({"value": e, "label": d.strftime("%a %d %b %Y")})
    return {"underlying": underlying.upper(), "expiries": out}


@app.post("/api/select")
def api_select(body: SelectBody):
    try:
        return tracker.select(body.underlying, body.expiry)
    except (LookupError, ValueError) as e:
        raise HTTPException(400, str(e))
    except RuntimeError as e:
        raise HTTPException(500, str(e))


class OrderBody(BaseModel):
    strike: float
    option_type: str
    side: str
    lots: int = 1


class OrdersBody(BaseModel):
    sel_id: int
    orders: list[OrderBody]
    strategy: str = ""


class IdBody(BaseModel):
    id: int


class SettleBody(BaseModel):
    id: int
    spot: float


def _live_price(leg):
    st = tracker.state
    if st is None or (st.underlying, st.expiry) != (leg["underlying"], leg["expiry"]):
        raise HTTPException(400, f"Switch to {leg['underlying']} {leg['expiry']} to close this "
                                 "position at a live price.")
    px = st.ltp_for(leg["strike"], leg["type"])
    if px is None:
        raise HTTPException(400, "No live price for this contract yet. Try again in a moment.")
    return px


@app.post("/api/paper/orders")
def paper_orders(body: OrdersBody):
    st = tracker.state
    if st is None:
        raise HTTPException(400, "Nothing is being tracked yet.")
    if st.sel_id != body.sel_id:
        raise HTTPException(409, "The chain changed while you were ordering. Try again.")
    priced = []
    for o in body.orders:        # price everything first so a basket is all-or-nothing
        px = st.ltp_for(o.strike, o.option_type)
        if px is None:
            raise HTTPException(400, f"No live price yet for {o.strike:g} {o.option_type.upper()}.")
        priced.append((o, px))
    for o, _ in priced:          # validate everything before placing anything
        if o.side.upper() not in ("BUY", "SELL") or o.option_type.upper() not in ("CE", "PE"):
            raise HTTPException(400, "Each leg needs Buy/Sell and CE/PE.")
        if not 1 <= o.lots <= 1000:
            raise HTTPException(400, "Lots must be between 1 and 1000.")
    name = body.strategy.strip()[:40]
    group = tracker.book.new_group() if (name or len(priced) > 1) else None
    if group and not name:
        name = "Strategy"
    try:
        legs = [tracker.book.place(st.underlying, st.expiry, o.strike, o.option_type,
                                   o.side, o.lots, st.lot, px, strategy=name if group else "",
                                   group=group) for o, px in priced]
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"placed": len(legs), "group": group}


@app.post("/api/paper/close")
def paper_close(body: IdBody):
    leg = tracker.book.find_open(body.id)
    if leg is None:
        raise HTTPException(404, "That position is already closed.")
    tracker.book.close(body.id, _live_price(leg))
    return {"ok": True}


@app.post("/api/paper/close_group")
def paper_close_group(body: IdBody):
    with tracker.book.lock:
        legs = [l for l in tracker.book.legs if l["status"] == "open" and l.get("group") == body.id]
        if not legs:
            raise HTTPException(404, "That strategy has no open legs.")
        priced = [(l, _live_price(l)) for l in legs]     # all-or-nothing on prices
        for l, px in priced:
            tracker.book.close(l["id"], px)
    return {"closed": len(legs)}


@app.post("/api/paper/settle")
def paper_settle(body: SettleBody):
    leg = tracker.book.find_open(body.id)
    if leg is None:
        raise HTTPException(404, "That position is already closed.")
    if not expired(leg["expiry"]):
        raise HTTPException(400, "Only expired contracts can be settled. Use Close for live ones.")
    iv = max(0.0, body.spot - leg["strike"]) if leg["type"] == "CE" else max(0.0, leg["strike"] - body.spot)
    tracker.book.close(body.id, iv)
    return {"ok": True, "settled_at": iv}


class StopBody(BaseModel):
    group: int | None = None
    direction: str
    level: float
    hold_secs: int = 60
    action: str = "exit"
    trail_pct: float = 0.0


@app.post("/api/stops")
def stops_add(body: StopBody):
    st = tracker.state
    if st is None:
        raise HTTPException(400, "Nothing is being tracked yet.")
    try:
        r = tracker.stops.add(st.underlying, st.expiry, body.group, body.direction,
                              body.level, body.hold_secs, body.action, st.spot,
                              body.trail_pct)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"id": r["id"], "label": r["label"]}


@app.post("/api/stops/cancel")
def stops_cancel(body: IdBody):
    try:
        tracker.stops.cancel(body.id)
    except LookupError as e:
        raise HTTPException(404, str(e))
    return {"ok": True}


@app.get("/api/notify")
def notify_status():
    return {"channels": tracker.notifier.channels(), "kinds": sorted(tracker.notifier.kinds)}


@app.post("/api/notify/test")
def notify_test():
    if not tracker.notifier.channels():
        raise HTTPException(400, "No phone channel is configured. See the Phone alerts section of the README.")
    return {"results": tracker.notifier.test()}


@app.post("/api/paper/reset")
def paper_reset():
    tracker.book.reset()
    return {"ok": True}


@app.get("/api/health")
def api_health():
    mgr = tracker.manager
    return {"status": mgr.status if mgr else "stopped", "today": date.today().isoformat()}


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    hub.clients[ws] = -1
    try:
        st = tracker.state
        if st is not None:
            hello = tracker.hello()
            await ws.send_text(_dumps(hello))
            hub.clients[ws] = hello["sel_id"]
        while True:
            await ws.receive_text()      # we never expect input; this detects disconnects
    except WebSocketDisconnect:
        pass
    except Exception:  # noqa: BLE001
        pass
    finally:
        hub.clients.pop(ws, None)


if __name__ == "__main__":
    # Binds to localhost only. Do not expose this to a network without adding auth.
    uvicorn.run(app, host=os.getenv("HOST", "127.0.0.1"), port=int(os.getenv("PORT", "8000")),
                log_level="warning")