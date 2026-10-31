"""
market_state.py
Thread-safe in-memory model of ONE option chain (one underlying + one expiry).

The consumer thread calls update() for every tick. The server calls snapshot()
once a second and sample_if_due() to build OI history. Everything the UI shows
(OI change windows, PCR, max pain, support/resistance) is computed here.

OI change windows
  d    "today": versus the first OI this tracker recorded today. The baseline is
       saved to ./cache so a restart keeps it. Start before 09:15 and it equals
       the change from the previous close.
  m5, m15, m30: versus the OI sampled that long ago (sampled every 10 s).
"""
from __future__ import annotations

import json
import logging
import threading
import time
from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
log = logging.getLogger("state")

WINDOWS = {"m5": 300, "m15": 900, "m30": 1800}
SAMPLE_EVERY = 10     # seconds between history samples
HIST_LEN = 200        # samples kept per token (~33 min)
SERIES_LEN = 3000     # intraday chart points kept (~8 h)


def _max_pain(strikes, ce_oi, pe_oi):
    best_k, best = None, None
    for k in strikes:
        pain = 0
        for s in strikes:
            if k > s:
                pain += (k - s) * ce_oi.get(s, 0)
            elif s > k:
                pain += (s - k) * pe_oi.get(s, 0)
        if best is None or pain < best:
            best_k, best = k, pain
    return best_k


class MarketState:
    def __init__(self, sel_id, underlying, expiry, instruments, spot_inst, cache_dir="./cache"):
        self.sel_id = sel_id
        self.underlying, self.expiry = underlying, expiry
        self.lot = next((i["lotsize"] for i in instruments if i.get("lotsize")), 0)
        self.opt = {i["token"]: i for i in instruments}
        self.by_contract = {(float(i["strike"]), i["option_type"]): i["token"] for i in instruments}
        self.spot_token = spot_inst["token"] if spot_inst else None
        self.spot_symbol = spot_inst["symbol"] if spot_inst else None

        self.lock = threading.RLock()
        self.ticks: dict[str, dict] = {}
        self.hist: dict[str, deque] = defaultdict(lambda: deque(maxlen=HIST_LEN))
        self.series: deque = deque(maxlen=SERIES_LEN)   # [ts, spot, ce_oi, pe_oi]
        self.spot = None
        self.spot_close = None
        self.spot_at = None          # wall-clock time of the latest spot tick
        self.last_tick_at = None
        self.first_sample_at = None
        self._last_sample = 0.0

        self.oi_base: dict[str, int] = {}
        self.baseline_at = None
        self._baseline_saved = False
        Path(cache_dir).mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(IST).strftime("%Y%m%d")
        self._baseline_file = Path(cache_dir) / f"baseline_{underlying}_{expiry}_{stamp}.json"
        self._load_baseline()

    # ---- baseline persistence -------------------------------------------- #
    def _load_baseline(self):
        try:
            if self._baseline_file.exists():
                data = json.loads(self._baseline_file.read_text())
                self.oi_base = {str(k): int(v) for k, v in data["oi"].items()}
                self.baseline_at = data.get("saved_at")
                self._baseline_saved = True
                log.info("Loaded OI baseline (%d tokens) from %s",
                         len(self.oi_base), self._baseline_file.name)
        except Exception:  # noqa: BLE001
            log.exception("Could not read baseline file; starting fresh")
            self.oi_base, self.baseline_at, self._baseline_saved = {}, None, False

    def _save_baseline(self):
        try:
            tmp = self._baseline_file.with_suffix(".tmp")
            tmp.write_text(json.dumps({"saved_at": self.baseline_at, "oi": self.oi_base}))
            tmp.replace(self._baseline_file)
            self._baseline_saved = True
            log.info("Saved OI baseline (%d tokens)", len(self.oi_base))
        except Exception:  # noqa: BLE001
            log.exception("Could not save baseline")

    def _maybe_save_baseline(self, now):
        if self._baseline_saved or not self.oi_base or self.baseline_at is None:
            return
        if len(self.oi_base) >= 0.9 * len(self.opt) or now - self.baseline_at > 60:
            self._save_baseline()

    # ---- ingest (consumer thread) ---------------------------------------- #
    def update(self, tick: dict):
        token = str(tick.get("token", "")).strip("\"'")
        ex = tick.get("exchange_type")
        now = time.time()
        ltp = (tick.get("last_traded_price") or 0) / 100.0   # paise -> rupees

        with self.lock:
            is_spot = (ex == 1) or (ex is None and token == self.spot_token and token not in self.opt)
            if is_spot:
                if token == self.spot_token and ltp > 0:
                    self.spot = ltp
                    self.spot_at = now
                    close = (tick.get("closed_price") or 0) / 100.0
                    if close > 0:
                        self.spot_close = close
                    self.last_tick_at = now
                return

            if token not in self.opt:
                return
            oi = int(tick.get("open_interest") or 0)
            rec = self.ticks.setdefault(token, {"ltp": 0.0, "oi": 0, "vol": 0, "close": 0.0})
            rec["ltp"] = ltp
            rec["oi"] = oi
            rec["vol"] = int(tick.get("volume_trade_for_the_day") or rec["vol"])
            close = (tick.get("closed_price") or 0) / 100.0
            if close > 0:
                rec["close"] = close
            if oi > 0 and token not in self.oi_base:
                self.oi_base[token] = oi
                if self.baseline_at is None:
                    self.baseline_at = now
            self.last_tick_at = now

    def ltp_for(self, strike, option_type):
        """Live LTP for one contract, or None if it has not traded / ticked yet."""
        with self.lock:
            tok = self.by_contract.get((float(strike), str(option_type).upper()))
            rec = self.ticks.get(tok) if tok else None
            return rec["ltp"] if rec and rec["ltp"] > 0 else None

    # ---- periodic sampling (server loop) --------------------------------- #
    def sample_if_due(self):
        now = time.time()
        with self.lock:
            if now - self._last_sample < SAMPLE_EVERY or not self.ticks:
                return
            self._last_sample = now
            if self.first_sample_at is None:
                self.first_sample_at = now
            ce = pe = 0
            for token, rec in self.ticks.items():
                self.hist[token].append((now, rec["oi"]))
                if self.opt[token]["option_type"] == "CE":
                    ce += rec["oi"]
                else:
                    pe += rec["oi"]
            self.series.append([int(now), self.spot, ce, pe])
            self._maybe_save_baseline(now)

    # ---- output ----------------------------------------------------------- #
    def _deltas(self, token, oi, now):
        out = {name: None for name in WINDOWS}
        h = self.hist.get(token)
        if not h:
            return out
        pending = dict(WINDOWS)
        for t, v in reversed(h):
            age = now - t
            for name, secs in list(pending.items()):
                if age >= secs:
                    out[name] = oi - v
                    del pending[name]
            if not pending:
                break
        return out

    def hello(self):
        with self.lock:
            return {
                "type": "hello", "sel_id": self.sel_id,
                "underlying": self.underlying, "expiry": self.expiry,
                "lot": self.lot, "spot_symbol": self.spot_symbol,
                "strikes": len({i["strike"] for i in self.opt.values()}),
                "series": list(self.series),
            }

    def snapshot(self):
        now = time.time()
        with self.lock:
            by_k: dict[float, dict] = {}
            ce_oi, pe_oi = {}, {}
            tchg = {w: {"ce": None, "pe": None} for w in ("d", *WINDOWS)}

            for token, inst in self.opt.items():
                rec = self.ticks.get(token)
                if rec is None:
                    continue
                k, side_key = inst["strike"], inst["option_type"].lower()
                oi = rec["oi"]
                chg = self._deltas(token, oi, now)
                base = self.oi_base.get(token)
                chg["d"] = (oi - base) if base is not None else None
                by_k.setdefault(k, {"k": k, "ce": None, "pe": None})[side_key] = {
                    "ltp": rec["ltp"], "oi": oi, "vol": rec["vol"],
                    "close": rec["close"], "chg": chg,
                }
                (ce_oi if side_key == "ce" else pe_oi)[k] = oi
                for w, v in chg.items():
                    if v is not None:
                        tchg[w][side_key] = (tchg[w][side_key] or 0) + v

            rows = [by_k[k] for k in sorted(by_k)]
            tot_ce, tot_pe = sum(ce_oi.values()), sum(pe_oi.values())

            atm = None
            if rows:
                if self.spot:
                    atm = min((r["k"] for r in rows), key=lambda k: abs(k - self.spot))
                else:  # no spot feed: strike where call and put prices are closest
                    both = [r for r in rows if r["ce"] and r["pe"]
                            and r["ce"]["ltp"] > 0 and r["pe"]["ltp"] > 0]
                    if both:
                        atm = min(both, key=lambda r: abs(r["ce"]["ltp"] - r["pe"]["ltp"]))["k"]

            strikes = sorted(set(ce_oi) | set(pe_oi))
            spot_chg = None
            if self.spot and self.spot_close:
                spot_chg = round((self.spot / self.spot_close - 1) * 100, 2)

            return {
                "type": "snap", "ts": int(now), "sel_id": self.sel_id,
                "underlying": self.underlying, "expiry": self.expiry, "lot": self.lot,
                "spot": self.spot, "spot_chg_pct": spot_chg, "atm": atm,
                "tick_age": None if self.last_tick_at is None else round(now - self.last_tick_at, 1),
                "warm": int(now - self.first_sample_at) if self.first_sample_at else 0,
                "baseline_at": self.baseline_at,
                "totals": {
                    "ce_oi": tot_ce, "pe_oi": tot_pe,
                    "pcr": round(tot_pe / tot_ce, 3) if tot_ce else None,
                    "chg": tchg,
                },
                "levels": {
                    "max_pain": _max_pain(strikes, ce_oi, pe_oi) if strikes else None,
                    "resistance": [[k, v] for k, v in
                                   sorted(ce_oi.items(), key=lambda kv: kv[1], reverse=True)[:3]],
                    "support": [[k, v] for k, v in
                                sorted(pe_oi.items(), key=lambda kv: kv[1], reverse=True)[:3]],
                },
                "rows": rows,
                "point": list(self.series[-1]) if self.series else None,
            }