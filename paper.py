"""
paper.py
Paper-trading book. Orders fill at the live LTP of the tracked chain.
Legs persist to ./cache/paper_book.json so they survive restarts.

Not modelled: brokerage, taxes, slippage, margin, bid/ask spread.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from datetime import date, datetime
from pathlib import Path

log = logging.getLogger("paper")


def expired(expiry: str) -> bool:
    try:
        return datetime.strptime(expiry, "%d%b%Y").date() < date.today()
    except ValueError:
        return False


def _pnl(leg: dict, px: float) -> float:
    sign = 1 if leg["side"] == "BUY" else -1
    return round(sign * (px - leg["entry"]) * leg["qty"], 2)


class PaperBook:
    def __init__(self, path):
        self.path = Path(path)
        self.lock = threading.RLock()
        self.legs: list[dict] = []
        self.next_id = 1
        self.next_group = 1
        self.pnl_series: deque = deque(maxlen=3000)   # [ts, total P&L], in memory only
        self._load()

    # ---- persistence ------------------------------------------------------ #
    def _load(self):
        try:
            if self.path.exists():
                d = json.loads(self.path.read_text())
                self.legs = d.get("legs", [])
                self.next_id = d.get("next_id", 1)
                top = max([l.get("group") or 0 for l in self.legs] + [0]) + 1
                self.next_group = max(d.get("next_group", 1), top)
                log.info("Loaded paper book: %d legs", len(self.legs))
        except Exception:  # noqa: BLE001
            log.exception("Could not read paper book; starting empty")

    def _save(self):
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps({"next_id": self.next_id, "next_group": self.next_group,
                                        "legs": self.legs}))
            tmp.replace(self.path)
        except Exception:  # noqa: BLE001
            log.exception("Could not save paper book")

    # ---- actions ---------------------------------------------------------- #
    def new_group(self) -> int:
        with self.lock:
            g = self.next_group
            self.next_group += 1
            return g

    def place(self, underlying, expiry, strike, opt, side, lots, lot, price,
              strategy: str = "", group: int | None = None) -> dict:
        side, opt = side.upper(), opt.upper()
        if side not in ("BUY", "SELL") or opt not in ("CE", "PE"):
            raise ValueError("Order needs a side (Buy/Sell) and a type (CE/PE).")
        if not 1 <= int(lots) <= 1000:
            raise ValueError("Lots must be between 1 and 1000.")
        if not price or price <= 0:
            raise ValueError("No live price for this contract yet.")
        if not lot:
            raise ValueError("Lot size is unknown for this contract.")
        with self.lock:
            leg = {
                "id": self.next_id, "underlying": underlying, "expiry": expiry,
                "strike": float(strike), "type": opt, "side": side,
                "lots": int(lots), "lot": int(lot), "qty": int(lots) * int(lot),
                "entry": round(float(price), 2), "opened": int(time.time()),
                "status": "open", "strategy": strategy, "group": group,
            }
            self.next_id += 1
            self.legs.append(leg)
            self._save()
            return leg

    def find_open(self, leg_id: int) -> dict | None:
        with self.lock:
            return next((l for l in self.legs if l["id"] == leg_id and l["status"] == "open"), None)

    def close(self, leg_id: int, price: float) -> dict:
        with self.lock:
            leg = self.find_open(leg_id)
            if leg is None:
                raise LookupError("That position is already closed or does not exist.")
            leg.update(status="closed", exit=round(float(price), 2),
                       closed=int(time.time()), pnl=_pnl(leg, price))
            self._save()
            return leg

    def reset(self):
        with self.lock:
            self.legs, self.next_id, self.next_group = [], 1, 1
            self.pnl_series.clear()
            self._save()

    # ---- views ------------------------------------------------------------ #
    def view(self, state) -> dict:
        with self.lock:
            sel = (state.underlying, state.expiry) if state else None
            open_legs, closed = [], []
            unreal = real = 0.0
            untracked = 0
            for l in self.legs:
                if l["status"] == "open":
                    live = sel == (l["underlying"], l["expiry"])
                    ltp = state.ltp_for(l["strike"], l["type"]) if live else None
                    pnl = _pnl(l, ltp) if ltp is not None else None
                    if live and pnl is not None:
                        unreal += pnl
                    if not live:
                        untracked += 1
                    open_legs.append({**l, "live": live, "ltp": ltp, "pnl": pnl,
                                      "expired": expired(l["expiry"])})
                else:
                    real += l["pnl"]
                    closed.append(l)
            return {
                "legs": open_legs, "closed": closed[-30:][::-1],
                "unrealized": round(unreal, 2), "realized": round(real, 2),
                "untracked": untracked,
            }

    def record(self, state):
        """Append a (timestamp, total P&L) sample for the session chart."""
        with self.lock:
            if not self.legs:
                return
            v = self.view(state)
            self.pnl_series.append([int(time.time()), round(v["unrealized"] + v["realized"], 2)])