# OI Ladder

Live option chain and open-interest dashboard fed by Angel One SmartStream.

```
oi-ladder/
  angel_core.py     scrip master cache, token mapping, login, stream + auto-reconnect
  market_state.py   chain model, OI change windows, PCR, max pain, support/resistance
  paper.py          paper-trading book (legs, P&L, saved to cache/paper_book.json)
  stops.py          stop-loss rules on spot: hold timer, alerts, auto exit (cache/stop_rules.json)
  notify.py         phone alerts: WhatsApp (CallMeBot or Twilio) and Telegram
  server.py         FastAPI app: REST + WebSocket push to the browser
  static/index.html the UI (no build step)
  requirements.txt
  .env.example
```

## Run

```bash
pip install -r requirements.txt
copy .env.example .env        # Windows (use cp on Linux/macOS), then fill in the 4 values
python server.py
```

Open http://127.0.0.1:8000. It starts on the nearest NIFTY expiry. Change the
underlying or expiry in the header and press Track.

## How it fits together

```
Angel One SmartStream
      |  on_data (fast, only enqueues)
      v
 queue.Queue --> TickConsumer thread --> MarketState.update()
                                              |
                      server loop, every 1 s: snapshot() / sample_if_due()
                                              v
                                  WebSocket /ws  -->  browser
```

- Option tokens come from the Scrip Master at runtime. Spot (index or equity) is
  resolved from the same file, so nothing is hardcoded.
- Options are subscribed in Full mode (OI, LTP, volume). Spot uses Quote mode.
- The browser gets one JSON snapshot per second plus the session history for the charts.

## What "OI change" means here

- **Today**: versus the first OI this tracker saw today. It is saved in `./cache`, so
  restarting keeps it. Start the server before 9:15 and it equals the change from the
  previous close. Delete `cache/baseline_*.json` to reset it.
- **5 / 15 / 30 min**: versus OI sampled that long ago. These show "Collecting…"
  until enough history exists.

## Paper trading

Open the **Paper trades** tab. The strategy builder and the payoff chart sit side by side.

1. **Build a draft.** Add legs one at a time (Buy or Sell, Call or Put, strike, lots), load a
   preset (straddle, strangle, iron condor, bull call spread, bear put spread), or click any
   LTP in the option chain and choose Add to strategy. In the draft you can flip Buy and
   Sell, flip CE and PE, step a strike up or down, and change lots.
2. **Watch the payoff update.** The chart shows the draft at expiry with max profit, max loss,
   breakevens and net credit or debit. The toggle switches between Draft, Open positions
   and Draft + open.
3. **Execute strategy.** All legs fill together at the live LTP and appear under
   **Your strategy positions**, grouped under the strategy name with a combined P&L, a
   Payoff button for that group and Close all.

Single orders from the chain ticket (Place paper order) appear under Single orders.

- Brokerage, taxes, slippage and margin are not modelled.
- Positions are saved in `cache/paper_book.json` and survive restarts. A leg can only be
  closed while its chain is tracked, since that is where its live price comes from. Legs
  past expiry can be settled by entering the final index price.
- "Reset book" deletes everything.

## Stop-loss on spot

On **Paper trades**, press **Stop-loss** on a strategy (or **Stop-loss for all**) and set:

- a direction and level: spot goes **above** or **below** it,
- a hold time: it must stay there for that long (immediately, 30 s, 1 min, 3 min, 5 min or custom),
- an action: **Notify and exit**, or **Notify only**.

The timer starts when spot crosses the level and resets if spot comes back. When the hold
time is met, the server exits the protected legs at the live LTP and books the P&L. This
runs on the server every second, so the exit still happens if the browser tab is closed.
The browser shows a toast, plays a sound and, if you allow it, sends a desktop notification.
The payoff chart marks armed stop levels with a red dashed line.

**Trailing stop.** Set *Trail the stop* to, for example, 0.5. Each time spot moves 0.5% in
your favour, the stop level moves the same distance the same way and you get a notification
(`trail` alerts, also sent to WhatsApp or Telegram). "In your favour" means away from the
stop: a stop set *below* spot trails upward as spot rises, and a stop set *above* spot trails
downward as spot falls. It only ratchets one way and never loosens. If spot jumps several
steps at once, the stop catches up in one go. Trailing pauses together with the rule.

A rule is paused (and its timer reset) when:
- its chain is not the one being tracked, because the spot price comes from the tracked chain,
- the market is closed (set `STOP_RULES_ALWAYS=1` in `.env` to test outside market hours),
- the spot price is more than 15 seconds old.

Limits: the server must be running, you only get a desktop notification while the page is
open, and the exit fills at the live LTP with no slippage. This is for paper trades only.

## Phone alerts (WhatsApp)

Stop-loss alerts can be sent to your phone, so they reach you with the browser closed. Add
the values for a channel to `.env` and restart. Then press **Send test message** under
Alerts on the Paper trades tab.

**WhatsApp via CallMeBot (simplest, no 24-hour limit)**
1. Follow the setup on callmebot.com for WhatsApp: you save their contact number, then send
   it the message `I allow callmebot to send me messages` from your WhatsApp. The bot replies
   with your API key. Use the number shown on their site, as it can change.
2. Set `CALLMEBOT_PHONE` (international format with +, for India `+91` then 10 digits) and
   `CALLMEBOT_APIKEY`.
3. This is a free third-party service, not an official WhatsApp product. Your alert text goes
   through their server, delivery is not guaranteed, and it is meant for personal use.

**WhatsApp via Twilio**
1. In the Twilio console, open the WhatsApp Sandbox and send the `join <your code>` message
   from your phone to the sandbox number.
2. Set `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_WHATSAPP_FROM` and `WHATSAPP_TO`.
3. Limits: the sandbox only delivers free-form messages within 24 hours of your last message
   to it, and the sandbox session expires after about three days. For lasting alerts you need
   an approved WhatsApp sender and a message template, which is a bigger setup.

**Telegram (most reliable, free)**
1. In Telegram, open **@BotFather**, send `/newbot`, choose a name and a username ending in
   `bot`. It replies with a token. Put it in `.env` as `TELEGRAM_BOT_TOKEN`.
2. Open your new bot and send it `/start`. A bot cannot message you until you message it first.
3. Run `python telegram_setup.py`. It prints the line `TELEGRAM_CHAT_ID=...`. Add it to `.env`.
4. Restart the server and press **Send test message** (or run `python telegram_setup.py --test`).

You can enable several channels at once; each alert goes to all of them. `NOTIFY_KINDS`
controls what is sent (default: `stop,exit,warn`). Warnings are limited to one per rule per
minute, each message is retried three times, and a failed delivery shows up in the in-app
Alerts list. Never commit `.env`.

## Notes

- OI "Units / Lots" toggle: Angel's feed may report OI in units or in lots. If the
  numbers look 25-75x too large against NSE, switch to Lots.
- The server binds to 127.0.0.1 and has no login. Do not expose it to a network as is.
- Python 3.11 or 3.12 is the safe choice. 3.14 is very new and some dependencies may lag.
- Not included: IV and Greeks, order placement, saving history to disk.