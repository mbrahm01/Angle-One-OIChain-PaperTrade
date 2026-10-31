"""
angel_core.py
Shared Angel One plumbing: Scrip Master cache + token mapping, login, SmartStream.
No UI code lives here; server.py imports it.
"""
from __future__ import annotations

import json
import logging
import queue
import threading
import time
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pyotp
import requests
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from dotenv import load_dotenv
import os

load_dotenv(Path(__file__).with_name(".env"))
log = logging.getLogger("angel")

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
SCRIP_MASTER_URL = (
    "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
)
CACHE_DIR = Path(os.getenv("SCRIP_CACHE_DIR", "./cache"))
IST = ZoneInfo("Asia/Kolkata")

NSE_CM = 1            # exchangeType for cash / index spot
NFO = 2               # exchangeType for NSE F&O
MODE_QUOTE = 2        # LTP + OHLC (used for spot)
MODE_FULL = 3         # SNAP_QUOTE: LTP + OI + volume + depth (used for options)
MAX_TOKENS = 1000     # SmartStream per-session subscription limit
QUEUE_MAXSIZE = 200_000
STALE_FEED_SECS = 45  # no packets this long during market hours -> reconnect
BACKOFF_START, BACKOFF_MAX = 2, 60

# Index name in the Scrip Master for each F&O underlying symbol.
INDEX_ALIASES = {
    "NIFTY": "NIFTY 50",
    "BANKNIFTY": "NIFTY BANK",
    "FINNIFTY": "NIFTY FIN SERVICE",
    "MIDCPNIFTY": "NIFTY MID SELECT",
}


# --------------------------------------------------------------------------- #
# Scrip master + token mapping
# --------------------------------------------------------------------------- #
_master_lock = threading.Lock()
_option_index: dict | None = None   # {(name, expiry): [instrument rows]}
_spot_index: dict = {}              # {("IDX"|"EQ", NAME): row}


def normalize_expiry(expiry) -> str:
    """Accepts date/datetime, '2026-10-29', '29OCT2026', '29-Oct-2026' -> '29OCT2026'."""
    if isinstance(expiry, (date, datetime)):
        return expiry.strftime("%d%b%Y").upper()
    s = str(expiry).strip()
    for fmt in ("%d%b%Y", "%Y-%m-%d", "%d-%m-%Y", "%d-%b-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%d%b%Y").upper()
        except ValueError:
            continue
    raise ValueError(f"Unrecognised expiry format: {expiry!r}")


def _download_scrip_master(dest: Path) -> None:
    log.info("Downloading Scrip Master -> %s", dest)
    tmp = dest.with_suffix(".tmp")
    with requests.get(SCRIP_MASTER_URL, stream=True, timeout=(10, 120)) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    os.replace(tmp, dest)  # atomic: never leave a half-written cache


def load_scrip_master(force: bool = False) -> None:
    """Load today's Scrip Master (downloaded once per day) and index it."""
    global _option_index, _spot_index
    with _master_lock:
        if _option_index is not None and not force:
            return
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        path = CACHE_DIR / f"OpenAPIScripMaster_{date.today():%Y%m%d}.json"
        if force or not path.exists():
            _download_scrip_master(path)
            for old in CACHE_DIR.glob("OpenAPIScripMaster_*.json"):
                if old != path:
                    old.unlink(missing_ok=True)
        else:
            log.info("Using cached Scrip Master: %s", path)

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list) or not data:
            path.unlink(missing_ok=True)
            raise RuntimeError("Scrip master is empty/malformed; cache removed, retry.")

        options = defaultdict(list)
        spots: dict = {}
        for row in data:
            seg, itype = row.get("exch_seg"), row.get("instrumenttype")
            if seg == "NFO" and itype in ("OPTIDX", "OPTSTK"):
                options[(row["name"], row["expiry"].upper())].append(row)
            elif seg == "NSE":
                if itype == "AMXIDX":
                    # Angel keeps the index label in "symbol" ("Nifty 50") and a short
                    # code in "name" ("NIFTY"); index both so either spelling resolves.
                    spots[("IDX", row["name"].upper())] = row
                    spots[("IDX", str(row.get("symbol", "")).upper())] = row
                elif str(row.get("symbol", "")).endswith("-EQ"):
                    spots[("EQ", row["name"].upper())] = row
        _option_index = dict(options)
        _spot_index = spots
        log.info("Scrip master loaded: %d rows, %d option buckets, %d spot instruments",
                 len(data), len(_option_index), len(_spot_index))


def list_underlyings() -> tuple[list[str], list[str]]:
    """(index underlyings, stock underlyings) that have listed options."""
    load_scrip_master()
    idx, stk = set(), set()
    for (name, _), rows in _option_index.items():
        (idx if rows[0].get("instrumenttype") == "OPTIDX" else stk).add(name)
    return sorted(idx), sorted(stk)


def list_expiries(underlying_symbol: str) -> list[str]:
    load_scrip_master()
    exps = {e for (n, e) in _option_index if n == underlying_symbol.upper()}
    return sorted(exps, key=lambda e: datetime.strptime(e, "%d%b%Y"))


def upcoming_expiries(underlying_symbol: str) -> list[str]:
    today = date.today()
    return [e for e in list_expiries(underlying_symbol)
            if datetime.strptime(e, "%d%b%Y").date() >= today]


def get_option_tokens(underlying_symbol: str, expiry_date) -> list[dict]:
    """[{token, symbol, strike, option_type, expiry, lotsize}, ...] sorted by strike."""
    load_scrip_master()
    name, expiry = underlying_symbol.upper(), normalize_expiry(expiry_date)
    out = []
    for r in _option_index.get((name, expiry), []):
        sym = r["symbol"]
        opt_type = sym[-2:]
        if opt_type not in ("CE", "PE"):
            continue
        out.append({
            "token": str(r["token"]),
            "symbol": sym,
            "strike": float(r["strike"]) / 100.0,   # master stores strike * 100
            "option_type": opt_type,
            "expiry": expiry,
            "lotsize": int(float(r.get("lotsize", 0) or 0)),
        })
    out.sort(key=lambda x: (x["strike"], x["option_type"]))
    if not out:
        raise LookupError(
            f"No NFO options for {name} at {expiry}. "
            f"Available expiries: {', '.join(list_expiries(name)[:8]) or 'none (unknown symbol?)'}"
        )
    return out


def get_spot_instrument(underlying_symbol: str) -> dict | None:
    """Spot index / equity instrument for an underlying, resolved from the master."""
    load_scrip_master()
    u = underlying_symbol.upper()
    row = _spot_index.get(("IDX", INDEX_ALIASES.get(u, u))) or _spot_index.get(("EQ", u))
    if row is None:
        log.warning("No spot instrument found for %s", u)
        return None
    return {"token": str(row["token"]), "symbol": row["symbol"], "exchange_type": NSE_CM}


def cap_instruments(instruments: list[dict], limit: int = MAX_TOKENS, center=None) -> list[dict]:
    """Keep the strikes nearest `center` (default: median strike) within the token limit."""
    if len(instruments) <= limit:
        return instruments
    per = defaultdict(int)
    for i in instruments:
        per[i["strike"]] += 1
    strikes = sorted(per)
    if center is None:
        center = strikes[len(strikes) // 2]
    strikes.sort(key=lambda s: abs(s - center))
    keep, n = set(), 0
    for s in strikes:
        if n + per[s] > limit:
            break
        keep.add(s)
        n += per[s]
    trimmed = [i for i in instruments if i["strike"] in keep]
    log.warning("Trimmed %d -> %d tokens to respect the %d-token limit",
                len(instruments), len(trimmed), limit)
    return trimmed


def market_open_now() -> bool:
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    return (9, 15) <= (now.hour, now.minute) <= (15, 30)


# --------------------------------------------------------------------------- #
# Authentication
# --------------------------------------------------------------------------- #
class AngelSession:
    def __init__(self, api_key, client_code, password, totp_secret):
        self.api_key, self.client_code = api_key, client_code
        self.password, self.totp_secret = password, totp_secret
        self.api: SmartConnect | None = None
        self.auth_token = self.feed_token = None

    @classmethod
    def from_env(cls) -> "AngelSession":
        try:
            return cls(
                os.getenv("ANGEL_API_KEY"), os.getenv("ANGEL_CLIENT_CODE"),
                os.getenv("ANGEL_PASSWORD"), os.getenv("ANGEL_TOTP_SECRET"),
            )
        except KeyError as e:
            raise RuntimeError(f"Missing environment variable {e}. Set it or add it to .env")

    def _totp(self) -> str:
        secret = "".join(self.totp_secret.split()).replace("-", "")
        secret = secret.replace('"', "").replace("'", "").upper()
        return pyotp.TOTP(secret).now()

    def login(self) -> None:
        self.api = SmartConnect(api_key=self.api_key)
        resp = self.api.generateSession(self.client_code, self.password, self._totp())
        if not resp or not resp.get("status"):
            msg = (resp or {}).get("message", resp)
            raise RuntimeError(f"Login failed: {msg}")
        d = resp["data"]
        self.auth_token = d["jwtToken"]
        self.feed_token = d.get("feedToken") or self.api.getfeedToken()
        log.info("Login OK for client %s", self.client_code)

    def logout(self) -> None:
        try:
            if self.api:
                self.api.terminateSession(self.client_code)
        except Exception as e:  # noqa: BLE001
            log.debug("Logout ignored error: %s", e)


# --------------------------------------------------------------------------- #
# Consumer thread: queue -> callback
# --------------------------------------------------------------------------- #
def _decode(raw):
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="ignore")
    if isinstance(raw, str):
        raw = json.loads(raw)
    return raw if isinstance(raw, dict) else None


class TickConsumer(threading.Thread):
    """Pulls packets off the queue and hands each decoded tick to `on_tick`."""

    def __init__(self, q: queue.Queue, on_tick):
        super().__init__(name="consumer", daemon=True)
        self.q, self.on_tick = q, on_tick
        self.count = 0
        self._halt = threading.Event()   # NB: not `_stop` - that shadows Thread internals

    def stop(self):
        self._halt.set()
        try:
            self.q.put_nowait(None)      # wake the loop
        except queue.Full:
            pass

    def run(self):
        while not self._halt.is_set():
            try:
                raw = self.q.get(timeout=1)
            except queue.Empty:
                continue
            if raw is None:
                continue
            try:
                tick = _decode(raw)
                if tick:
                    self.on_tick(tick)
                    self.count += 1
            except Exception:  # noqa: BLE001 - one bad packet must not kill the thread
                log.exception("Failed to process tick: %r", raw)


# --------------------------------------------------------------------------- #
# WebSocket manager with supervised reconnect
# --------------------------------------------------------------------------- #
class StreamManager:
    """
    subscriptions: [(mode, [{"exchangeType": int, "tokens": [str, ...]}, ...]), ...]

    The WebSocket callbacks only set flags / push to the queue. A supervisor loop
    (run_forever) tears the socket down, logs in again with a fresh TOTP and token,
    and reconnects with exponential backoff.
    """

    def __init__(self, session: AngelSession, subscriptions: list, q: queue.Queue):
        self.session, self.subscriptions, self.q = session, subscriptions, q
        self.stop_event = threading.Event()
        self.reconnect_event = threading.Event()
        self.status = "connecting"        # connecting | live | reconnecting | stopped
        self.last_error: str | None = None
        self.dropped = 0
        self._last_data = 0.0             # monotonic time of last packet
        self._opened_at = 0.0
        self._ws: SmartWebSocketV2 | None = None
        self._ws_thread: threading.Thread | None = None

    # ---- callbacks (WebSocket thread: keep them tiny) --------------------- #
    def _on_open(self, wsapp, *args):
        n = sum(len(g["tokens"]) for _, groups in self.subscriptions for g in groups)
        log.info("WebSocket open; subscribing %d tokens", n)
        self._opened_at = time.monotonic()
        self.status, self.last_error = "live", None
        seq = 0
        for mode, groups in self.subscriptions:
            for g in groups:
                toks = g["tokens"]
                for i in range(0, len(toks), 200):
                    self._ws.subscribe(
                        f"s{seq}", mode,
                        [{"exchangeType": g["exchangeType"], "tokens": toks[i:i + 200]}],
                    )
                    seq += 1

    def _on_data(self, wsapp, message, *args):
        self._last_data = time.monotonic()
        try:
            self.q.put_nowait(message)
        except queue.Full:
            self.dropped += 1
            if self.dropped % 1000 == 1:
                log.error("Queue full - dropped %d ticks so far", self.dropped)

    def _on_error(self, *args):
        err = args[-1] if args else "unknown"
        log.error("WebSocket error: %s", err)
        self.last_error = f"Stream error: {err}"[:160]
        self.reconnect_event.set()

    def _on_close(self, *args):
        log.warning("WebSocket closed")
        if not self.stop_event.is_set():
            self.reconnect_event.set()

    # ---- lifecycle -------------------------------------------------------- #
    def _start_ws(self):
        s = self.session
        ws = SmartWebSocketV2(s.auth_token, s.api_key, s.client_code, s.feed_token,
                              max_retry_attempt=0)   # we own reconnection
        ws.on_open, ws.on_data = self._on_open, self._on_data
        ws.on_error, ws.on_close = self._on_error, self._on_close
        self._ws = ws
        self._ws_thread = threading.Thread(target=ws.connect, name="ws", daemon=True)
        self._ws_thread.start()

    def _teardown_ws(self):
        ws, th = self._ws, self._ws_thread
        self._ws = self._ws_thread = None
        if ws:
            try:
                ws.close_connection()
            except Exception as e:  # noqa: BLE001
                log.debug("close_connection error ignored: %s", e)
        if th and th.is_alive():
            th.join(timeout=5)

    def run_forever(self):
        backoff = BACKOFF_START
        while not self.stop_event.is_set():
            try:
                self.reconnect_event.clear()
                self.status = "connecting"
                self.session.login()                      # fresh tokens every (re)connect
                self._last_data = time.monotonic()
                self._opened_at = 0.0
                self._start_ws()
                backoff = BACKOFF_START

                while not self.stop_event.is_set():
                    if self.reconnect_event.wait(timeout=5):
                        break
                    if self._opened_at and market_open_now():
                        idle = time.monotonic() - max(self._last_data, self._opened_at)
                        if idle > STALE_FEED_SECS:
                            log.warning("No packets for %.0fs in market hours - reconnecting", idle)
                            self.last_error = f"No data for {idle:.0f}s, reconnecting"
                            break
            except Exception as e:  # noqa: BLE001
                log.exception("Connection cycle failed")
                self.last_error = f"{type(e).__name__}: {e}"[:160]

            self._teardown_ws()
            if self.stop_event.is_set():
                break
            self.status = "reconnecting"
            log.info("Reconnecting in %ds ...", backoff)
            if self.stop_event.wait(backoff):
                break
            backoff = min(backoff * 2, BACKOFF_MAX)
        self.status = "stopped"

    def shutdown(self):
        self.stop_event.set()
        self.reconnect_event.set()
        self._teardown_ws()
        self.status = "stopped"
