"""
notify.py
Push stop-loss alerts to your phone. Channels switch on automatically when their
environment variables are set (see .env.example):

  WhatsApp via CallMeBot : CALLMEBOT_PHONE, CALLMEBOT_APIKEY
  WhatsApp via Twilio    : TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN,
                           TWILIO_WHATSAPP_FROM, WHATSAPP_TO
  Telegram               : TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

Sending happens on a background thread, so a slow network never blocks the
once-a-second stop-loss loop. Each message is tried 3 times per channel.
Secrets are scrubbed from log lines and error text.
"""
from __future__ import annotations

import logging
import os
import queue
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

log = logging.getLogger("notify")
IST = ZoneInfo("Asia/Kolkata")
TIMEOUT = 10
RETRY_WAITS = (1, 3)               # waits between the 3 attempts
WARN_MIN_INTERVAL = 60             # at most one "warn" message per rule per minute
FAIL_ALERT_INTERVAL = 600          # at most one in-app "delivery failed" alert per 10 min
TITLES = {"stop": "STOP-LOSS TRIGGERED", "exit": "POSITION EXITED",
          "trail": "Stop moved", "warn": "Heads up", "info": "Update"}


class Notifier:
    def __init__(self, on_fail=None):
        env = os.environ
        self.on_fail = on_fail          # callable(channel_name, detail)
        self.kinds = {k.strip() for k in env.get("NOTIFY_KINDS", "stop,exit,warn,trail").split(",") if k.strip()}
        self._senders: dict[str, callable] = {}
        self._secrets: list[str] = []
        self._q: queue.Queue = queue.Queue(maxsize=200)
        self._last_warn: dict = {}
        self._last_fail = 0.0

        phone, key = env.get("CALLMEBOT_PHONE", "").strip(), env.get("CALLMEBOT_APIKEY", "").strip()
        if phone and key:
            self._secrets += [key]
            self._senders["WhatsApp (CallMeBot)"] = lambda text, p=phone, k=key: self._callmebot(p, k, text)

        sid, tok = env.get("TWILIO_ACCOUNT_SID", "").strip(), env.get("TWILIO_AUTH_TOKEN", "").strip()
        frm, to = env.get("TWILIO_WHATSAPP_FROM", "").strip(), env.get("WHATSAPP_TO", "").strip()
        if sid and tok and frm and to:
            self._secrets += [tok]
            self._senders["WhatsApp (Twilio)"] = lambda text: self._twilio(sid, tok, frm, to, text)

        bot, chat = env.get("TELEGRAM_BOT_TOKEN", "").strip(), env.get("TELEGRAM_CHAT_ID", "").strip()
        if bot and chat:
            self._secrets += [bot]
            self._senders["Telegram"] = lambda text: self._telegram(bot, chat, text)

        if self._senders:
            log.info("Notifications on: %s (kinds: %s)", ", ".join(self._senders), ",".join(sorted(self.kinds)))
            threading.Thread(target=self._worker, name="notify", daemon=True).start()
        else:
            log.info("No phone notification channel configured (see .env.example)")

    # ---- public ----------------------------------------------------------- #
    def channels(self) -> list[str]:
        return list(self._senders)

    def send(self, alert: dict):
        """Called for every in-app alert; queues it if it should go to the phone."""
        if not self._senders:
            return
        kind = alert.get("kind")
        if kind not in self.kinds:      # "system" alerts are never forwarded
            return
        if kind == "warn":
            key = alert.get("rule")
            now = time.time()
            if now - self._last_warn.get(key, 0) < WARN_MIN_INTERVAL:
                return
            self._last_warn[key] = now
        stamp = datetime.now(IST).strftime("%H:%M:%S IST")
        text = f"{TITLES.get(kind, 'Alert')}\n{alert['text']}\n{stamp}"[:900]
        try:
            self._q.put_nowait(text)
        except queue.Full:
            log.error("Notification queue full; dropping a message")

    def test(self) -> list[dict]:
        """Send a test message to every channel and report what happened."""
        text = "OI Ladder test message. If you can read this, phone alerts are working."
        out = []
        for name, fn in self._senders.items():
            try:
                fn(text)
                out.append({"channel": name, "ok": True, "detail": "sent"})
            except Exception as e:  # noqa: BLE001
                out.append({"channel": name, "ok": False, "detail": self._scrub(e)})
        return out

    # ---- worker ----------------------------------------------------------- #
    def _worker(self):
        while True:
            text = self._q.get()
            for name, fn in self._senders.items():
                err = None
                for attempt in range(3):
                    try:
                        fn(text)
                        err = None
                        break
                    except Exception as e:  # noqa: BLE001
                        err = self._scrub(e)
                        if attempt < 2:
                            time.sleep(RETRY_WAITS[attempt])
                if err:
                    log.error("%s delivery failed: %s", name, err)
                    now = time.time()
                    if self.on_fail and now - self._last_fail > FAIL_ALERT_INTERVAL:
                        self._last_fail = now
                        try:
                            self.on_fail(name, err)
                        except Exception:  # noqa: BLE001
                            log.exception("on_fail callback failed")

    def _scrub(self, e) -> str:
        s = f"{type(e).__name__}: {e}" if not isinstance(e, RuntimeError) else str(e)
        for secret in self._secrets:
            if secret:
                s = s.replace(secret, "***")
        return s[:200]

    # ---- channels --------------------------------------------------------- #
    @staticmethod
    def _callmebot(phone, key, text):
        r = requests.get("https://api.callmebot.com/whatsapp.php",
                         params={"phone": phone, "text": text, "apikey": key}, timeout=TIMEOUT)
        body = r.text.strip()
        if r.status_code != 200 or "error" in body.lower():
            raise RuntimeError(f"CallMeBot replied {r.status_code}: {body[:120]}")

    @staticmethod
    def _twilio(sid, token, frm, to, text):
        wa = lambda n: n if n.startswith("whatsapp:") else "whatsapp:" + n   # noqa: E731
        r = requests.post(f"https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json",
                          data={"From": wa(frm), "To": wa(to), "Body": text},
                          auth=(sid, token), timeout=TIMEOUT)
        if r.status_code not in (200, 201):
            try:
                msg = r.json().get("message", r.text)
            except ValueError:
                msg = r.text
            raise RuntimeError(f"Twilio replied {r.status_code}: {str(msg)[:140]}")

    @staticmethod
    def _telegram(bot, chat, text):
        r = requests.post(f"https://api.telegram.org/bot{bot}/sendMessage",
                          json={"chat_id": chat, "text": text}, timeout=TIMEOUT)
        if r.status_code != 200:
            raise RuntimeError(f"Telegram replied {r.status_code}: {r.text[:140]}")