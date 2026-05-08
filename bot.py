"""
Tecxo-style Ads Broadcasting Bot
- Main bot: control panel (aiogram-style via python-telegram-bot)
- Userbots: Pyrogram clients that actually send ads to all groups
- Logs bot: separate bot that DMs logs to the owner
"""

import asyncio
import logging
import os
import sqlite3
from datetime import datetime
from typing import Dict, Optional

from dotenv import load_dotenv
from pyrogram import Client
from pyrogram.errors import (
    SessionPasswordNeeded, PhoneCodeInvalid, PhoneCodeExpired,
    PhoneNumberInvalid, FloodWait, ChatWriteForbidden,
    ChannelPrivate, UserDeactivated, AuthKeyUnregistered,
)
from pyrogram.enums import ChatType
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters, ConversationHandler,
)

# ---------- ENV ----------
load_dotenv()
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
LOGS_BOT_TOKEN = os.getenv("LOGS_BOT_TOKEN", "")
ADMINS = [int(x) for x in os.getenv("ADMINS", "").split(",") if x.strip().isdigit()]
MAX_ACCOUNTS = int(os.getenv("MAX_ACCOUNTS", "5"))

DATA_DIR = os.environ.get("DATA_DIR") or ("/data" if os.path.isdir("/data") else ".")
os.makedirs(DATA_DIR, exist_ok=True)
HEALTH_PORT = int(os.environ.get("PORT", "8080"))
SESSIONS_DIR = os.path.join(DATA_DIR, "sessions")
os.makedirs(SESSIONS_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "bot.db")

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("tecxo")

# ---------- DB ----------
def db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def init_db():
    with db() as con:
        con.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            ad_text TEXT,
            interval INTEGER DEFAULT 300,
            running INTEGER DEFAULT 0,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            phone TEXT,
            session_string TEXT,
            tg_user_id INTEGER,
            tg_first_name TEXT,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            account_id INTEGER,
            sent INTEGER DEFAULT 0,
            failed INTEGER DEFAULT 0,
            updated_at TEXT
        );
        """)

def ensure_user(user_id: int):
    with db() as con:
        con.execute(
            "INSERT OR IGNORE INTO users(user_id, created_at) VALUES (?, ?)",
            (user_id, datetime.utcnow().isoformat()),
        )

def get_user(user_id: int):
    with db() as con:
        return con.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()

def update_user(user_id: int, **kwargs):
    if not kwargs:
        return
    keys = ", ".join(f"{k}=?" for k in kwargs)
    with db() as con:
        con.execute(f"UPDATE users SET {keys} WHERE user_id=?", (*kwargs.values(), user_id))

def list_accounts(user_id: int):
    with db() as con:
        return con.execute(
            "SELECT * FROM accounts WHERE user_id=? ORDER BY id", (user_id,)
        ).fetchall()

def add_account(user_id, phone, session_string, tg_user_id, first_name):
    with db() as con:
        cur = con.execute(
            "INSERT INTO accounts(user_id, phone, session_string, tg_user_id, tg_first_name, created_at) "
            "VALUES (?,?,?,?,?,?)",
            (user_id, phone, session_string, tg_user_id, first_name, datetime.utcnow().isoformat()),
        )
        return cur.lastrowid

def delete_account(user_id, account_id):
    with db() as con:
        con.execute("DELETE FROM accounts WHERE id=? AND user_id=?", (account_id, user_id))
        con.execute("DELETE FROM stats WHERE account_id=?", (account_id,))

def bump_stats(user_id, account_id, sent=0, failed=0):
    with db() as con:
        row = con.execute(
            "SELECT id FROM stats WHERE user_id=? AND account_id=?",
            (user_id, account_id),
        ).fetchone()
        now = datetime.utcnow().isoformat()
        if row:
            con.execute(
                "UPDATE stats SET sent=sent+?, failed=failed+?, updated_at=? WHERE id=?",
                (sent, failed, now, row["id"]),
            )
        else:
            con.execute(
                "INSERT INTO stats(user_id, account_id, sent, failed, updated_at) VALUES (?,?,?,?,?)",
                (user_id, account_id, sent, failed, now),
            )

def get_stats(user_id):
    with db() as con:
        return con.execute(
            "SELECT a.phone, a.tg_first_name, COALESCE(s.sent,0) sent, COALESCE(s.failed,0) failed "
            "FROM accounts a LEFT JOIN stats s ON s.account_id=a.id "
            "WHERE a.user_id=?", (user_id,),
        ).fetchall()

# ---------- Logs Bot ----------
class LogsBot:
    def __init__(self, token: str):
        self.app: Optional[Application] = None
        self.token = token
        self.started_users = set()

    async def start(self):
        if not self.token:
            return
        self.app = Application.builder().token(self.token).build()
        self.app.add_handler(CommandHandler("start", self._on_start))
        await self.app.initialize()
        await self.app.start()
        await self.app.updater.start_polling(drop_pending_updates=True)
        log.info("Logs bot started")

    async def _on_start(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        self.started_users.add(update.effective_user.id)
        await update.message.reply_text(
            "✅ Logs activated. You will receive ad delivery logs here."
        )

    async def send(self, user_id: int, text: str):
        if not self.app:
            return
        try:
            await self.app.bot.send_message(user_id, text, parse_mode=ParseMode.HTML)
        except Exception as e:
            log.warning(f"Logs send failed for {user_id}: {e}")

logs_bot = LogsBot(LOGS_BOT_TOKEN)

# ---------- Userbot Manager ----------
class UserbotManager:
    """Holds running Pyrogram clients & broadcast loops per account."""
    def __init__(self):
        self.clients: Dict[int, Client] = {}      # account_id -> Client
        self.tasks: Dict[int, asyncio.Task] = {}  # account_id -> task

    async def get_or_create(self, account_row) -> Client:
        aid = account_row["id"]
        if aid in self.clients:
            return self.clients[aid]
        c = Client(
            name=f"acc_{aid}",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=account_row["session_string"],
            in_memory=True,
        )
        await c.start()
        self.clients[aid] = c
        return c

    async def stop_account(self, account_id: int):
        t = self.tasks.pop(account_id, None)
        if t:
            t.cancel()
        c = self.clients.pop(account_id, None)
        if c:
            try:
                await c.stop()
            except Exception:
                pass

    async def start_user(self, user_id: int):
        u = get_user(user_id)
        if not u or not u["ad_text"]:
            return
        for acc in list_accounts(user_id):
            if acc["id"] in self.tasks:
                continue
            self.tasks[acc["id"]] = asyncio.create_task(
                self._loop(user_id, dict(acc), u["interval"], u["ad_text"])
            )
        update_user(user_id, running=1)

    async def stop_user(self, user_id: int):
        for acc in list_accounts(user_id):
            await self.stop_account(acc["id"])
        update_user(user_id, running=0)

    async def _loop(self, user_id: int, acc: dict, interval: int, ad_text: str):
        await logs_bot.send(
            user_id, f"▶️ <b>Started</b> account <code>{acc['phone']}</code>"
        )
        while True:
            try:
                u = get_user(user_id)
                if not u or not u["running"]:
                    break
                client = await self.get_or_create(acc)
                groups = []
                async for d in client.get_dialogs():
                    if d.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
                        groups.append(d.chat)
                await logs_bot.send(
                    user_id,
                    f"📤 <code>{acc['phone']}</code> broadcasting to <b>{len(groups)}</b> groups…",
                )
                # PARALLEL SEND — sab groups me ek saath ad jaye
                ad_msg = u["ad_text"] or ad_text
                stop_flag = {"banned": False}

                async def _send_one(chat):
                    if stop_flag["banned"]:
                        return ("skip", chat.title)
                    try:
                        await client.send_message(chat.id, ad_msg)
                        await logs_bot.send(
                            user_id,
                            f"✅ <code>{acc['phone']}</code> → <b>{chat.title}</b>",
                        )
                        return ("ok", chat.title)
                    except FloodWait as e:
                        await logs_bot.send(
                            user_id,
                            f"⏳ FloodWait {e.value}s on <b>{chat.title}</b>, retrying…",
                        )
                        await asyncio.sleep(e.value + 1)
                        try:
                            await client.send_message(chat.id, ad_msg)
                            await logs_bot.send(
                                user_id,
                                f"✅ <code>{acc['phone']}</code> → <b>{chat.title}</b>",
                            )
                            return ("ok", chat.title)
                        except Exception as e2:
                            await logs_bot.send(
                                user_id,
                                f"⚠️ <b>{chat.title}</b>: {type(e2).__name__}",
                            )
                            return ("fail", chat.title)
                    except (ChatWriteForbidden, ChannelPrivate):
                        await logs_bot.send(
                            user_id,
                            f"🚫 No permission in <b>{chat.title}</b>",
                        )
                        return ("fail", chat.title)
                    except (UserDeactivated, AuthKeyUnregistered):
                        await logs_bot.send(
                            user_id,
                            f"❌ Account <code>{acc['phone']}</code> logged out / banned. Stopping.",
                        )
                        stop_flag["banned"] = True
                        return ("banned", chat.title)
                    except Exception as e:
                        await logs_bot.send(
                            user_id,
                            f"⚠️ <b>{chat.title}</b>: {type(e).__name__}",
                        )
                        return ("fail", chat.title)

                # fire ALL sends concurrently
                results = await asyncio.gather(
                    *[_send_one(chat) for chat in groups],
                    return_exceptions=False,
                )
                if stop_flag["banned"]:
                    await self.stop_account(acc["id"])
                    return
                sent = sum(1 for r in results if r[0] == "ok")
                failed = sum(1 for r in results if r[0] == "fail")
                bump_stats(user_id, acc["id"], sent=sent, failed=failed)
                await logs_bot.send(
                    user_id,
                    f"🔁 Cycle done for <code>{acc['phone']}</code> — "
                    f"✅ {sent} | ❌ {failed}. Sleeping <b>{interval}s</b>.",
                )
                # honor latest interval
                u2 = get_user(user_id)
                await asyncio.sleep(u2["interval"] if u2 else interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.exception("loop error")
                await logs_bot.send(user_id, f"💥 Loop error: {e}")
                await asyncio.sleep(10)
        await logs_bot.send(user_id, f"⏸ <b>Stopped</b> <code>{acc['phone']}</code>")

ubm = UserbotManager()

# ---------- Login conversation states ----------
PHONE, CODE, PASSWORD = range(3)
SET_AD, SET_INTERVAL = range(10, 12)

# pending logins: user_id -> {client, phone, sent_code}
pending_logins: Dict[int, dict] = {}

# ---------- Keyboards ----------
def main_menu():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("📊 Dashboard")],
            [KeyboardButton("📢 Updates"), KeyboardButton("🆘 Support")],
            [KeyboardButton("ℹ️ How To Use")],
            [KeyboardButton("⚡ Powered by")],
        ],
        resize_keyboard=True,
    )

def dashboard_menu():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("➕ Add Accounts"), KeyboardButton("👥 My Accounts")],
            [KeyboardButton("✍️ Set Ad Message"), KeyboardButton("⏱ Set Time Interval")],
            [KeyboardButton("▶️ Start Ads"), KeyboardButton("⏸ Stop Ads")],
            [KeyboardButton("🗑 Delete Accounts"), KeyboardButton("📈 Analytics")],
            [KeyboardButton("⬅️ Back")],
        ],
        resize_keyboard=True,
    )

# ---------- Handlers ----------
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    ensure_user(uid)
    text = (
        "🦊 <b>Welcome to Tecxo-style Free Ads bot</b> — <i>The Future of Telegram Automation</i>\n\n"
        "• Premium Ad Broadcasting\n"
        "• Smart Delays\n"
        "• Multi-Account Support\n\n"
        "For support contact: @YourSupport"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=main_menu())

async def menu_router(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    txt = (update.message.text or "").strip()
    uid = update.effective_user.id
    ensure_user(uid)

    if txt == "📊 Dashboard":
        return await show_dashboard(update, ctx)
    if txt == "⬅️ Back":
        return await update.message.reply_text("Main menu:", reply_markup=main_menu())
    if txt == "📢 Updates":
        return await update.message.reply_text("Follow @YourUpdates for updates.")
    if txt == "🆘 Support":
        return await update.message.reply_text("Contact @YourSupport for help.")
    if txt == "ℹ️ How To Use":
        return await update.message.reply_text(
            "1. Dashboard → Add Accounts (login phone + OTP)\n"
            "2. Set Ad Message\n"
            "3. Set Time Interval (seconds)\n"
            "4. Start Ads ▶️\n"
            "5. Open the Logs Bot and tap /start to receive logs."
        )
    if txt == "⚡ Powered by":
        return await update.message.reply_text("Powered by Tecxo-style automation.")

    # Dashboard buttons
    if txt == "➕ Add Accounts":
        return await begin_add_account(update, ctx)
    if txt == "👥 My Accounts":
        return await show_accounts(update, ctx)
    if txt == "✍️ Set Ad Message":
        ctx.user_data["awaiting"] = "ad"
        return await update.message.reply_text("Send the ad message now (text). Send /cancel to abort.")
    if txt == "⏱ Set Time Interval":
        ctx.user_data["awaiting"] = "interval"
        return await update.message.reply_text("Send interval in seconds (e.g. 300). Min 60, max 86400.")
    if txt == "▶️ Start Ads":
        return await start_ads(update, ctx)
    if txt == "⏸ Stop Ads":
        return await stop_ads(update, ctx)
    if txt == "🗑 Delete Accounts":
        return await delete_accounts_menu(update, ctx)
    if txt == "📈 Analytics":
        return await analytics(update, ctx)

    # awaiting input
    aw = ctx.user_data.get("awaiting")
    if aw == "ad":
        update_user(uid, ad_text=txt)
        ctx.user_data.pop("awaiting", None)
        return await update.message.reply_text("✅ Ad message saved.")
    if aw == "interval":
        if not txt.isdigit():
            return await update.message.reply_text("❌ Send a number in seconds.")
        v = max(60, min(86400, int(txt)))
        update_user(uid, interval=v)
        ctx.user_data.pop("awaiting", None)
        return await update.message.reply_text(f"✅ Interval set to {v}s.")

    # login flow
    if uid in pending_logins:
        return await handle_login_input(update, ctx)

async def show_dashboard(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    u = get_user(uid)
    accs = list_accounts(uid)
    status = "Running ▶️" if u and u["running"] else "Paused ⏸"
    ad = "Set ✅" if u and u["ad_text"] else "Not Set ⭕"
    interval = u["interval"] if u else 300
    text = (
        "<b>📊 Ads DASHBOARD</b>\n\n"
        f"• Hosted Accounts: <b>{len(accs)}/{MAX_ACCOUNTS}</b>\n"
        f"• Ad Message: <b>{ad}</b>\n"
        f"• Cycle Interval: <b>{interval}s</b>\n"
        f"• Advertising Status: <b>{status}</b>\n\n"
        "Choose an action below to continue 👇"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=dashboard_menu())

async def show_accounts(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    accs = list_accounts(uid)
    if not accs:
        return await update.message.reply_text("No accounts hosted yet.")
    lines = ["<b>👥 Your Accounts</b>"]
    for a in accs:
        lines.append(f"• <code>{a['phone']}</code> — {a['tg_first_name'] or ''}")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)

async def analytics(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    rows = get_stats(update.effective_user.id)
    if not rows:
        return await update.message.reply_text("No data yet.")
    lines = ["<b>📈 Analytics</b>"]
    total_s = total_f = 0
    for r in rows:
        total_s += r["sent"]; total_f += r["failed"]
        lines.append(f"• <code>{r['phone']}</code> — ✅ {r['sent']} | ❌ {r['failed']}")
    lines.append(f"\n<b>Total</b>: ✅ {total_s} | ❌ {total_f}")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)

async def delete_accounts_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    accs = list_accounts(uid)
    if not accs:
        return await update.message.reply_text("No accounts to delete.")
    kb = [
        [InlineKeyboardButton(f"🗑 {a['phone']}", callback_data=f"del:{a['id']}")]
        for a in accs
    ]
    await update.message.reply_text(
        "Tap an account to delete:", reply_markup=InlineKeyboardMarkup(kb)
    )

async def cb_delete(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    aid = int(q.data.split(":")[1])
    await ubm.stop_account(aid)
    delete_account(uid, aid)
    await q.edit_message_text("✅ Account deleted.")

# ---------- Add Account flow ----------
async def begin_add_account(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if len(list_accounts(uid)) >= MAX_ACCOUNTS:
        return await update.message.reply_text(f"❌ Max {MAX_ACCOUNTS} accounts reached.")
    pending_logins[uid] = {"step": "phone"}
    await update.message.reply_text(
        "📱 Send your phone number with country code (e.g. <code>+919876543210</code>).\n"
        "Send /cancel to abort.",
        parse_mode=ParseMode.HTML,
    )

async def handle_login_input(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    state = pending_logins.get(uid)
    if not state:
        return
    text = update.message.text.strip()

    if text == "/cancel":
        c = state.get("client")
        if c:
            try: await c.disconnect()
            except: pass
        pending_logins.pop(uid, None)
        return await update.message.reply_text("❌ Cancelled.")

    try:
        if state["step"] == "phone":
            phone = text
            client = Client(
                name=f"login_{uid}",
                api_id=API_ID,
                api_hash=API_HASH,
                in_memory=True,
            )
            await client.connect()
            sent = await client.send_code(phone)
            state.update(client=client, phone=phone, code_hash=sent.phone_code_hash, step="code")
            await update.message.reply_text(
                "🔐 Telegram sent you a code. Enter it with spaces between digits "
                "(e.g. <code>1 2 3 4 5</code>) so Telegram doesn't auto-revoke it.",
                parse_mode=ParseMode.HTML,
            )

        elif state["step"] == "code":
            code = text.replace(" ", "")
            client: Client = state["client"]
            try:
                signed_in = await client.sign_in(state["phone"], state["code_hash"], code)
            except SessionPasswordNeeded:
                state["step"] = "password"
                return await update.message.reply_text("🔑 2FA enabled. Send your password:")
            except (PhoneCodeInvalid, PhoneCodeExpired) as e:
                pending_logins.pop(uid, None)
                await client.disconnect()
                return await update.message.reply_text(f"❌ {type(e).__name__}. Try again.")
            await finish_login(update, uid, client, state["phone"], signed_in)

        elif state["step"] == "password":
            client: Client = state["client"]
            signed_in = await client.check_password(text)
            await finish_login(update, uid, client, state["phone"], signed_in)

    except PhoneNumberInvalid:
        pending_logins.pop(uid, None)
        await update.message.reply_text("❌ Invalid phone number.")
    except Exception as e:
        log.exception("login error")
        pending_logins.pop(uid, None)
        await update.message.reply_text(f"❌ Error: {e}")

async def finish_login(update: Update, uid: int, client: Client, phone: str, me):
    session_string = await client.export_session_string()
    await client.disconnect()
    pending_logins.pop(uid, None)
    add_account(uid, phone, session_string, me.id, me.first_name or "")
    await update.message.reply_text(
        f"✅ Logged in as <b>{me.first_name}</b> (<code>{phone}</code>).",
        parse_mode=ParseMode.HTML,
    )

# ---------- Start / Stop ----------
async def start_ads(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    u = get_user(uid)
    if not u or not u["ad_text"]:
        return await update.message.reply_text("❌ Set an ad message first.")
    if not list_accounts(uid):
        return await update.message.reply_text("❌ Add at least one account first.")
    await ubm.start_user(uid)
    await update.message.reply_text(
        "▶️ Ads started. Open the Logs Bot and tap /start to see live logs."
    )

async def stop_ads(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    await ubm.stop_user(uid)
    await update.message.reply_text("⏸ Ads stopped.")

async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    pending_logins.pop(uid, None)
    ctx.user_data.pop("awaiting", None)
    await update.message.reply_text("Cancelled.", reply_markup=main_menu())

# ---------- Healthcheck HTTP server (for Koyeb / Fly free tier) ----------
async def _start_health_server():
    from aiohttp import web
    async def ok(_req): return web.Response(text="ok")
    app = web.Application()
    app.router.add_get("/", ok)
    app.router.add_get("/health", ok)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", HEALTH_PORT)
    await site.start()
    log.info(f"Health server on :{HEALTH_PORT}")

# ---------- Main ----------
async def main():
    if not all([API_ID, API_HASH, BOT_TOKEN]):
        raise SystemExit("Missing API_ID / API_HASH / BOT_TOKEN env vars")
    init_db()

    await _start_health_server()
    await logs_bot.start()

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CallbackQueryHandler(cb_delete, pattern=r"^del:"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, menu_router))

    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)
    log.info("Main bot started. Polling…")

    # auto-resume running users
    with db() as con:
        running = con.execute("SELECT user_id FROM users WHERE running=1").fetchall()
    for r in running:
        try:
            await ubm.start_user(r["user_id"])
        except Exception as e:
            log.warning(f"resume failed {r['user_id']}: {e}")

    # block forever
    stop_event = asyncio.Event()
    try:
        await stop_event.wait()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
        if logs_bot.app:
            await logs_bot.app.updater.stop()
            await logs_bot.app.stop()
            await logs_bot.app.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
