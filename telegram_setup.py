"""
telegram_setup.py
Finds your Telegram chat ID and sends a test message.

  1. Put TELEGRAM_BOT_TOKEN in .env (from @BotFather).
  2. Open your bot in Telegram and send it any message, for example /start.
  3. Run:  python telegram_setup.py
     It prints the TELEGRAM_CHAT_ID line to paste into .env.
  4. Run:  python telegram_setup.py --test     (after adding the chat ID)
"""
import os
import sys

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import requests

TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()


def api(method, **payload):
    try:
        r = requests.post(f"https://api.telegram.org/bot{TOKEN}/{method}", json=payload, timeout=15)
    except requests.RequestException as e:
        # the URL contains the token, so never print the raw exception text
        sys.exit(f"Could not reach Telegram ({type(e).__name__}). Check your internet connection.")
    try:
        data = r.json()
    except ValueError:
        sys.exit(f"Telegram sent an unexpected reply (HTTP {r.status_code}).")
    if not data.get("ok"):
        sys.exit(f"Telegram refused the request: {data.get('description', 'unknown error')}")
    return data["result"]


def main():
    if not TOKEN:
        sys.exit("TELEGRAM_BOT_TOKEN is not set. Add it to .env first (get it from @BotFather).")

    me = api("getMe")
    print(f"Bot OK: @{me.get('username')}")

    if "--test" in sys.argv:
        chat = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
        if not chat:
            sys.exit("TELEGRAM_CHAT_ID is not set. Run this script without --test first.")
        api("sendMessage", chat_id=chat, text="OI Ladder test message. Telegram alerts are working.")
        print("Test message sent. Check Telegram.")
        return

    updates = api("getUpdates")
    chats = {}
    for u in updates:
        msg = u.get("message") or u.get("channel_post") or u.get("my_chat_member", {})
        chat = msg.get("chat") if isinstance(msg, dict) else None
        if chat:
            name = chat.get("title") or " ".join(filter(None, [chat.get("first_name"), chat.get("last_name")])) \
                or chat.get("username") or "unknown"
            chats[chat["id"]] = (chat.get("type", "?"), name)

    if not chats:
        sys.exit(f"No messages found. Open @{me.get('username')} in Telegram, send it /start, "
                 "then run this script again.")

    print("\nChats that have messaged your bot:")
    for cid, (kind, name) in chats.items():
        print(f"  {cid}   ({kind}: {name})")
    first = next(iter(chats))
    print(f"\nAdd this line to .env, then restart the server:\n\n  TELEGRAM_CHAT_ID={first}\n")
    if len(chats) > 1:
        print("More than one chat was found. Use the id of the chat where you want the alerts.")


if __name__ == "__main__":
    main()