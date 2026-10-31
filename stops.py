"""
stops.py
Time-confirmed stop-loss rules on the underlying's SPOT price.

A rule says: "if spot stays ABOVE (or BELOW) <level> continuously for <hold_secs>,
notify me and, optionally, exit the protected paper positions at the live LTP."

Optional trailing: with trail_pct = 0.5, every time spot moves 0.5% in your favour
(away from the stop) the stop level moves the same distance in the same direction and
you are notified. It only ratchets one way and never loosens.

  - The timer starts when spot crosses the level and resets if spot comes back.
  - Rules are evaluated on the server once a second, so the exit still happens if
    the browser tab is closed. Notifications reach the browser through the snapshot.
  - A rule is only evaluated while its chain is the one being tracked (that is where
    the spot and option prices come from), during market hours, and while the spot
    feed is fresh. Otherwise it is paused and its timer is reset.
  - Rules persist in ./cache/stop_rules.json. Alerts are in memory only.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from pathlib import Path

log = logging.getLogger("stops")

SPOT_MAX_AGE = 15        # seconds; staler spot pauses the rule and resets its timer
CLEAR_NOTICE_AFTER = 5   # only announce "back inside" if the breach lasted this long
EXIT_WARN_AFTER = 60     # warn if an exit still can't get live prices after this long


def _dur(s) -> str:
    s = int(s)
    if s <= 0:
        return "no wait"
    if s < 60:
        return f"{s}s"
    m, r = divmod(s, 60)
    return f"{m} min" + (f" {r}s" if r else "")


def _money(x: float) -> str:
    return f"{'-' if x < 0 else '+'}Rs {abs(x):,.0f}"


class StopRules:
    def __init__(self, path, book, on_alert=None):
        self.path = Path(path)
        self.book = book
        self.on_alert = on_alert     # callable(alert dict), e.g. a phone notifier
        self.lock = threading.RLock()
        self.rules: list[dict] = []
        self.next_id = 1
        self.alerts: deque = deque(maxlen=50)
        self.next_alert = 1
        self._load()

    # ---- persistence ------------------------------------------------------ #
    def _load(self):
        try:
            if self.path.exists():
                d = json.loads(self.path.read_text())
                self.rules = d.get("rules", [])
                self.next_id = d.get("next_id", 1)
                log.info("Loaded %d stop-loss rules", len(self.rules))
        except Exception:  # noqa: BLE001
            log.exception("Could not read stop rules; starting empty")

    def _save(self):
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            live = [r for r in self.rules if r["status"] in ("armed", "exiting")]
            done = [r for r in self.rules if r["status"] not in ("armed", "exiting")][-100:]
            keep = [{k: v for k, v in r.items() if not k.startswith("_")} for r in live + done]
            keep.sort(key=lambda r: r["id"])
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps({"next_id": self.next_id, "rules": keep}))
            tmp.replace(self.path)
        except Exception:  # noqa: BLE001
            log.exception("Could not save stop rules")

    def _alert(self, kind: str, text: str, rule_id: int | None = None, now: float | None = None):
        with self.lock:
            a = {"id": self.next_alert, "ts": int(now or time.time()), "kind": kind,
                 "text": text, "rule": rule_id}
            self.next_alert += 1
            self.alerts.append(a)
        log.warning("ALERT [%s] %s", kind, text)
        if self.on_alert:
            try:
                self.on_alert(a)
            except Exception:  # noqa: BLE001 - a notifier bug must not break the stop loop
                log.exception("Alert hook failed")

    # ---- management ------------------------------------------------------- #
    def add(self, underlying, expiry, group, direction, level, hold_secs, action, spot,
            trail_pct: float = 0.0) -> dict:
        direction, action = str(direction).lower(), str(action).lower()
        if direction not in ("above", "below"):
            raise ValueError("Direction must be above or below.")
        if action not in ("exit", "alert"):
            raise ValueError("Action must be exit or alert.")
        if not level or level <= 0:
            raise ValueError("Enter a level above zero.")
        if not 0 <= int(hold_secs) <= 6 * 3600:
            raise ValueError("Hold time must be between 0 seconds and 6 hours.")
        trail_pct = float(trail_pct or 0)
        if not 0 <= trail_pct <= 10:
            raise ValueError("Trail must be between 0% and 10%.")
        if spot is None:
            raise ValueError("No live spot price yet, so the level can't be checked. Try again shortly.")
        if (direction == "above" and spot >= level) or (direction == "below" and spot <= level):
            raise ValueError(f"Spot ({spot:,.2f}) is already {direction} {level:,.2f}. "
                             "Pick a level it has not crossed yet.")
        with self.lock, self.book.lock:
            legs = self._legs(underlying, expiry, group)
            if not legs:
                raise ValueError("There are no open positions to protect on this chain.")
            label = (f"{legs[0].get('strategy') or 'Strategy'} #{group}"
                     if group else "All open positions")
            r = {
                "id": self.next_id, "underlying": underlying, "expiry": expiry,
                "group": group, "label": label, "direction": direction,
                "level": float(level), "hold_secs": int(hold_secs), "action": action,
                "trail_pct": trail_pct, "anchor": float(spot), "start_level": float(level),
                "moves": 0,
                "status": "armed", "outcome": "", "created": int(time.time()),
                "triggered_at": None,
            }
            self.next_id += 1
            self.rules.append(r)
            self._save()
            return r

    def cancel(self, rule_id: int):
        with self.lock:
            r = next((x for x in self.rules if x["id"] == rule_id
                      and x["status"] in ("armed", "exiting")), None)
            if r is None:
                raise LookupError("That rule is not active.")
            r.update(status="cancelled", outcome="Cancelled by you")
            r["_since"], r["_held"] = None, 0
            self._save()

    # ---- evaluation ------------------------------------------------------- #
    def _legs(self, underlying, expiry, group):
        return [l for l in self.book.legs
                if l["status"] == "open" and l["underlying"] == underlying
                and l["expiry"] == expiry and (group is None or l.get("group") == group)]

    def _targets(self, r):
        return self._legs(r["underlying"], r["expiry"], r["group"])

    def evaluate(self, state, active: bool = True, now: float | None = None):
        """Call once a second."""
        now = now or time.time()
        with self.lock, self.book.lock:
            dirty = False
            for r in self.rules:
                if r["status"] not in ("armed", "exiting"):
                    continue
                targets = self._targets(r)
                if not targets:
                    if r["status"] == "exiting":
                        self._finish_exit(r, now)
                    else:
                        r.update(status="cancelled",
                                 outcome="Positions were closed before the stop triggered")
                    r["_since"], r["_held"] = None, 0
                    dirty = True
                    continue
                if r["status"] == "exiting":
                    self._try_exit(r, state, targets, now)
                    dirty = True
                    continue

                live = state is not None and (state.underlying, state.expiry) == (r["underlying"], r["expiry"])
                spot = state.spot if live else None
                spot_at = getattr(state, "spot_at", None) if live else None
                if not live:
                    pause = "chain"
                elif not active:
                    pause = "market"
                elif spot is None or spot_at is None or now - spot_at > SPOT_MAX_AGE:
                    pause = "feed"
                else:
                    pause = None
                r["_pause"] = pause
                if pause:
                    r["_since"], r["_held"] = None, 0
                    continue

                if r.get("trail_pct"):
                    dirty |= self._trail(r, spot, now)

                breached = spot > r["level"] if r["direction"] == "above" else spot < r["level"]
                since = r.get("_since")
                if breached:
                    if since is None:
                        r["_since"] = since = now
                        if r["hold_secs"] > 0:
                            tail = ("Exiting" if r["action"] == "exit" else "You will be notified")
                            self._alert("warn",
                                        f"{r['label']}: {r['underlying']} spot {spot:,.2f} is {r['direction']} "
                                        f"{r['level']:,.2f}. {tail} in {_dur(r['hold_secs'])} if it stays there.",
                                        r["id"], now)
                    r["_held"] = int(now - since)
                    if now - since >= r["hold_secs"]:
                        self._trigger(r, state, spot, now)
                        dirty = True
                else:
                    if since is not None and now - since >= CLEAR_NOTICE_AFTER:
                        self._alert("info",
                                    f"{r['label']}: spot {spot:,.2f} is back inside {r['level']:,.2f}. "
                                    "Timer reset.", r["id"], now)
                    r["_since"], r["_held"] = None, 0
            if dirty:
                self._save()

    def _trail(self, r, spot, now) -> bool:
        """Ratchet the stop when spot has moved trail_pct in the favourable direction."""
        pct = r["trail_pct"]
        favourable_up = r["direction"] == "below"     # stop below spot protects against a fall
        old, steps = r["level"], 0
        while steps < 200:
            step = r["anchor"] * pct / 100.0
            if favourable_up and spot >= r["anchor"] + step:
                r["anchor"] += step
                r["level"] += step
            elif not favourable_up and spot <= r["anchor"] - step:
                r["anchor"] -= step
                r["level"] -= step
            else:
                break
            steps += 1
        if not steps:
            return False
        r["level"], r["anchor"] = round(r["level"], 2), round(r["anchor"], 2)
        r["moves"] = r.get("moves", 0) + steps
        way = "up" if favourable_up else "down"
        self._alert("trail",
                    f"{r['label']}: {r['underlying']} moved {pct:g}% in your favour"
                    f"{f' ({steps} steps)' if steps > 1 else ''}, spot {spot:,.2f}. "
                    f"Stop moved {way} from {old:,.2f} to {r['level']:,.2f} "
                    f"({abs(spot - r['level']):,.0f} pts from spot).", r["id"], now)
        return True

    def _trigger(self, r, state, spot, now):
        r["triggered_at"] = int(now)
        how = (f"for {_dur(r['hold_secs'])}" if r["hold_secs"] else "(no hold time)")
        base = (f"STOP-LOSS: {r['label']}. {r['underlying']} spot {spot:,.2f} has been "
                f"{r['direction']} {r['level']:,.2f} {how}.")
        if r["action"] == "alert":
            r.update(status="done", outcome="Notified only. Positions left open.")
            self._alert("stop", base + " Positions left open (notify only).", r["id"], now)
        else:
            r.update(status="exiting")
            r["_exit_started"], r["_closed"], r["_booked"] = now, 0, 0.0
            self._alert("stop", base + " Exiting at live prices.", r["id"], now)
            self._try_exit(r, state, self._targets(r), now)

    def _try_exit(self, r, state, targets, now):
        live = state is not None and (state.underlying, state.expiry) == (r["underlying"], r["expiry"])
        for l in list(targets):
            px = state.ltp_for(l["strike"], l["type"]) if live else None
            if px is None:
                continue
            try:
                closed = self.book.close(l["id"], px)
            except LookupError:
                continue
            r["_closed"] = r.get("_closed", 0) + 1
            r["_booked"] = r.get("_booked", 0.0) + closed["pnl"]
        remaining = self._targets(r)
        if not remaining:
            self._finish_exit(r, now)
        elif not r.get("_warned") and now - r.get("_exit_started", now) > EXIT_WARN_AFTER:
            r["_warned"] = True
            self._alert("warn",
                        f"{r['label']}: still waiting for live prices to exit {len(remaining)} leg(s). "
                        f"Check the feed, and keep {r['underlying']} {r['expiry']} selected.",
                        r["id"], now)

    def _finish_exit(self, r, now):
        n, booked = r.get("_closed", 0), r.get("_booked", 0.0)
        if n:
            r.update(status="done", outcome=f"Exited {n} leg{'s' if n != 1 else ''}, booked {_money(booked)}")
            self._alert("exit", f"{r['label']}: exited {n} leg{'s' if n != 1 else ''} at live prices. "
                                f"Booked P&L {_money(booked)}.", r["id"], now)
        else:
            r.update(status="done", outcome="Positions were already closed")

    # ---- views ------------------------------------------------------------ #
    def view(self, state) -> list[dict]:
        with self.lock:
            sel = (state.underlying, state.expiry) if state else None
            out = []
            for r in self.rules:
                d = {k: v for k, v in r.items() if not k.startswith("_")}
                d["held"] = r.get("_held", 0)
                d["pause"] = r.get("_pause")
                d["live"] = sel == (r["underlying"], r["expiry"])
                out.append(d)
            active = [d for d in out if d["status"] in ("armed", "exiting")]
            recent = [d for d in out if d["status"] not in ("armed", "exiting")][-8:][::-1]
            return active + recent

    def alert_list(self) -> list[dict]:
        with self.lock:
            return list(self.alerts)[-30:]