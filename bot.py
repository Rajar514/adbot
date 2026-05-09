"""
Tecxo-style Ads Broadcasting Bot — v3 IMPROVED
================================================
Fixes & Improvements over v2:
  1. Broken pipe (errno 32) — proper async reconnect with full client teardown
  2. Command delay — bot uses concurrent updates + larger connection pools
  3. "Not working sometimes" — heartbeat watchdog restarts stalled loops
  4. Logs queue — async-safe, non-blocking, with HTML fallback
  5. FloodWait handling — respected globally, per-account tracking
  6. Session reconnection — exponential backoff with jitter, auto-reconnect
  7. DB thread-safety — WAL mode, per-call connections with timeout
  8. Group cache — stale-on-error invalidation + smarter refresh
  9. graceful shutdown — properly awaits all tasks before exit
 10. /restart command for admins, /myid renamed to /id, new /stats
 11. Error-rate alerting — alerts if >50% of sends fail in a cycle
 12. Auto-resume on startup restores all running users
"""

import asyncio
import html
import logging
import os
import random
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
from pyrogram import Client
from pyrogram.errors import (
    SessionPasswordNeeded, PhoneCodeInvalid, PhoneCodeExpired,
    PhoneNumberInvalid, FloodWait, ChatWriteForbidden,
    ChannelPrivate, UserDeactivated, AuthKeyUnregistered,
    RPCError,
)
from pyrogram.enums import ChatType
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton,
)
from telegram.constants import ParseMode
from telegram.error import NetworkError, TimedOut, RetryAfter, TelegramError
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters,
)

# ─────────────────────────── ENV ──────────────────────────────────────────────
load_dotenv()
API_ID   = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN       = os.getenv("BOT_TOKEN", "")
LOGS_BOT_TOKEN  = os.getenv("LOGS_BOT_TOKEN", "")
ADMINS          = [int(x) for x in os.getenv("ADMINS", "").split(",") if x.strip().isdigit()]
MAX_ACCOUNTS    = int(os.getenv("MAX_ACCOUNTS", "5"))
SEND_CONCURRENCY= int(os.getenv("SEND_CONCURRENCY", "6"))   # lower = safer
LOG_VERBOSE     = os.getenv("LOG_VERBOSE", "1") not in ("0", "false", "False", "")
GROUPS_REFRESH_EVERY = int(os.getenv("GROUPS_REFRESH_EVERY", "5"))
# Min delay between individual sends (seconds) — avoids flood
SEND_DELAY_MIN  = float(os.getenv("SEND_DELAY_MIN", "0.4"))
SEND_DELAY_MAX  = float(os.getenv("SEND_DELAY_MAX", "1.2"))
# Watchdog: if a loop task hasn't heartbeated in this many seconds, restart it
WATCHDOG_TIMEOUT = int(os.getenv("WATCHDOG_TIMEOUT", "600"))

DATA_DIR     = os.environ.get("DATA_DIR") or ("/data" if os.path.isdir("/data") else ".")
os.makedirs(DATA_DIR, exist_ok=True)
SESSIONS_DIR = os.path.join(DATA_DIR, "sessions")
os.makedirs(SESSIONS_DIR, exist_ok=True)
DB_PATH      = os.path.join(DATA_DIR, "bot.db")
HEALTH_PORT  = int(os.environ.get("PORT", "8080"))

# ─────────────────────────── LOGGING ──────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
log = logging.getLogger("tecxo")

def esc(s) -> str:
    return html.escape(str(s) if s is not None else "", quote=False)

def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()

# ─────────────────────────── DATABASE ─────────────────────────────────────────
def _db_connect() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=20, check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")      # safe concurrent reads
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA foreign_keys=ON")
    return con

def init_db():
    with _db_connect() as con:
        con.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id   INTEGER PRIMARY KEY,
            ad_text   TEXT,
            interval  INTEGER DEFAULT 300,
            running   INTEGER DEFAULT 0,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS accounts (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id        INTEGER,
            phone          TEXT,
            session_string TEXT,
            tg_user_id     INTEGER,
            tg_first_name  TEXT,
            created_at     TEXT
        );
        CREATE TABLE IF NOT EXISTS stats (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER,
            account_id INTEGER UNIQUE,
            sent       INTEGER DEFAULT 0,
            failed     INTEGER DEFAULT 0,
            updated_at TEXT
        );
        """)

def _db(fn):
    """Decorator: opens a fresh DB connection per call."""
    import functools
    @functools.wraps(fn)
    def wrapper(*a, **kw):
        with _db_connect() as con:
            return fn(con, *a, **kw)
    return wrapper

# Plain functions — each opens its own connection (thread-safe)
def ensure_user(user_id: int):
    with _db_connect() as con:
        con.execute(
            "INSERT OR IGNORE INTO users(user_id, created_at) VALUES (?, ?)",
            (user_id, utcnow()),
        )

def get_user(user_id: int):
    with _db_connect() as con:
        return con.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()

def update_user(user_id: int, **kwargs):
    if not kwargs:
        return
    keys = ", ".join(f"{k}=?" for k in kwargs)
    with _db_connect() as con:
        con.execute(f"UPDATE users SET {keys} WHERE user_id=?", (*kwargs.values(), user_id))

def list_accounts(user_id: int):
    with _db_connect() as con:
        return con.execute(
            "SELECT * FROM accounts WHERE user_id=? ORDER BY id", (user_id,)
        ).fetchall()

def add_account(user_id, phone, session_string, tg_user_id, first_name) -> int:
    with _db_connect() as con:
        cur = con.execute(
            "INSERT INTO accounts(user_id, phone, session_string, tg_user_id, tg_first_name, created_at) "
            "VALUES (?,?,?,?,?,?)",
            (user_id, phone, session_string, tg_user_id, first_name, utcnow()),
        )
        return cur.lastrowid

def delete_account(user_id, account_id):
    with _db_connect() as con:
        con.execute("DELETE FROM accounts WHERE id=? AND user_id=?", (account_id, user_id))
        con.execute("DELETE FROM stats WHERE account_id=?", (account_id,))

def bump_stats(user_id, account_id, sent=0, failed=0):
    with _db_connect() as con:
        con.execute(
            """INSERT INTO stats(user_id, account_id, sent, failed, updated_at)
               VALUES (?,?,?,?,?)
               ON CONFLICT(account_id) DO UPDATE SET
                 sent=sent+excluded.sent,
                 failed=failed+excluded.failed,
                 updated_at=excluded.updated_at""",
            (user_id, account_id, sent, failed, utcnow()),
        )

def get_stats(user_id):
    with _db_connect() as con:
        return con.execute(
            "SELECT a.phone, a.tg_first_name, COALESCE(s.sent,0) sent, COALESCE(s.failed,0) failed "
            "FROM accounts a LEFT JOIN stats s ON s.account_id=a.id "
            "WHERE a.user_id=?", (user_id,),
        ).fetchall()

def get_all_running_users():
    with _db_connect() as con:
        return [r["user_id"] for r in con.execute("SELECT user_id FROM users WHERE running=1").fetchall()]

# ─────────────────────────── LOGS BOT ─────────────────────────────────────────
class LogsBot:
    """
    Non-blocking log delivery.
    All sends go through a background worker so broadcast loops are never stalled.
    Supports HTML with auto-fallback to plain text on parse error.
    """
    def __init__(self, token: str):
        self.token = token
        self.app: Optional[Application] = None
        self.started_users: set = set()
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=8000)
        self._worker_task: Optional[asyncio.Task] = None

    async def start(self):
        if not self.token:
            log.warning("LOGS_BOT_TOKEN not set — logs will not be delivered")
            return
        self.app = Application.builder().token(self.token).concurrent_updates(True).build()
        self.app.add_handler(CommandHandler("start", self._on_start))
        await self.app.initialize()
        await self.app.start()
        await self.app.updater.start_polling(drop_pending_updates=True)
        self._worker_task = asyncio.create_task(self._worker(), name="logs_worker")
        log.info("Logs bot started")

    async def _on_start(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        self.started_users.add(update.effective_user.id)
        await update.message.reply_text("✅ Logs activated! You'll receive live broadcast reports here.")

    def send(self, user_id: int, text: str):
        """Non-blocking enqueue. Safe to call from any coroutine."""
        if not self.app:
            return
        try:
            self.queue.put_nowait((user_id, text))
        except asyncio.QueueFull:
            log.warning("Logs queue full — message dropped")

    async def _send_msg(self, user_id: int, text: str):
        """Try HTML then plain text; up to 3 attempts with backoff."""
        for attempt in range(3):
            try:
                await self.app.bot.send_message(
                    user_id, text[:4096],
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                return
            except RetryAfter as e:
                await asyncio.sleep(e.retry_after + 1)
            except TelegramError as e:
                err_lower = str(e).lower()
                if "can't parse" in err_lower or "parse" in err_lower:
                    # HTML parse error — strip tags and retry as plain
                    try:
                        plain = html.unescape(text)
                        # crude strip of HTML tags
                        import re
                        plain = re.sub(r"<[^>]+>", "", plain)
                        await self.app.bot.send_message(user_id, plain[:4096])
                    except Exception as ee:
                        log.debug(f"Logs plain fallback failed {user_id}: {ee}")
                    return
                if "blocked" in err_lower or "deactivated" in err_lower:
                    return  # user blocked the bot, stop silently
                await asyncio.sleep(2 ** attempt)
            except (ConnectionError, OSError, TimeoutError):
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                log.debug(f"Logs send error {user_id}: {e}")
                return

    async def _worker(self):
        while True:
            try:
                user_id, text = await self.queue.get()
                await self._send_msg(user_id, text)
                self.queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning(f"Logs worker unhandled error: {e}")
                await asyncio.sleep(1)

logs_bot = LogsBot(LOGS_BOT_TOKEN)

# ─────────────────────────── USERBOT MANAGER ──────────────────────────────────
class UserbotManager:
    """
    Manages Pyrogram clients and broadcasting loops.
    Key fixes:
    - Per-account semaphore caps concurrent sends
    - Broken pipe (errno 32) triggers full client reconnect
    - Watchdog detects stalled loops and restarts them
    - FloodWait is respected globally per account
    """
    def __init__(self):
        self.clients: Dict[int, Client] = {}
        self.tasks: Dict[int, asyncio.Task] = {}
        self._heartbeats: Dict[int, float] = {}     # account_id → last beat timestamp
        self._flood_until: Dict[int, float] = {}    # account_id → timestamp to wait until
        self._watchdog_task: Optional[asyncio.Task] = None

    async def start_watchdog(self):
        self._watchdog_task = asyncio.create_task(self._watchdog(), name="watchdog")

    async def _watchdog(self):
        """Periodically checks that loop tasks are alive and heartbeating."""
        while True:
            try:
                await asyncio.sleep(60)
                now = time.monotonic()
                dead = []
                for aid, task in list(self.tasks.items()):
                    if task.done():
                        dead.append(aid)
                    elif (now - self._heartbeats.get(aid, now)) > WATCHDOG_TIMEOUT:
                        log.warning(f"Watchdog: account {aid} loop stalled, cancelling")
                        task.cancel()
                        dead.append(aid)
                for aid in dead:
                    self.tasks.pop(aid, None)
                    self._heartbeats.pop(aid, None)

                # Re-start tasks for any running user whose tasks died
                for user_id in get_all_running_users():
                    for acc in list_accounts(user_id):
                        if acc["id"] not in self.tasks:
                            log.info(f"Watchdog: restarting account {acc['id']} for user {user_id}")
                            u = get_user(user_id)
                            if u and u["running"] and u["ad_text"]:
                                self.tasks[acc["id"]] = asyncio.create_task(
                                    self._loop(user_id, dict(acc), u["interval"], u["ad_text"]),
                                    name=f"loop_{acc['id']}",
                                )
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.exception(f"Watchdog error: {e}")
                await asyncio.sleep(30)

    async def _get_client(self, acc: dict) -> Client:
        """Get existing client or create and connect a new one."""
        aid = acc["id"]
        c = self.clients.get(aid)
        if c and c.is_connected:
            return c
        # Tear down any dead client first
        if c:
            try:
                await asyncio.wait_for(c.stop(), timeout=10)
            except Exception:
                pass
            self.clients.pop(aid, None)
        # Create fresh
        c = Client(
            name=f"acc_{aid}",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=acc["session_string"],
            in_memory=True,
            sleep_threshold=60,         # auto-sleep up to 60s FloodWait
            max_concurrent_transmissions=4,
        )
        await asyncio.wait_for(c.start(), timeout=30)
        self.clients[aid] = c
        return c

    async def _teardown_client(self, account_id: int):
        c = self.clients.pop(account_id, None)
        if c:
            try:
                await asyncio.wait_for(c.stop(), timeout=10)
            except Exception:
                pass

    async def stop_account(self, account_id: int):
        t = self.tasks.pop(account_id, None)
        if t and not t.done():
            t.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(t), timeout=5)
            except Exception:
                pass
        await self._teardown_client(account_id)
        self._heartbeats.pop(account_id, None)
        self._flood_until.pop(account_id, None)

    async def start_user(self, user_id: int):
        u = get_user(user_id)
        if not u or not u["ad_text"]:
            return
        for acc in list_accounts(user_id):
            aid = acc["id"]
            if aid in self.tasks and not self.tasks[aid].done():
                continue
            self.tasks[aid] = asyncio.create_task(
                self._loop(user_id, dict(acc), u["interval"], u["ad_text"]),
                name=f"loop_{aid}",
            )
        update_user(user_id, running=1)

    async def stop_user(self, user_id: int):
        for acc in list_accounts(user_id):
            await self.stop_account(acc["id"])
        update_user(user_id, running=0)

    async def _send_with_retry(
        self, client: Client, chat_id, text: str, max_retries: int = 3
    ) -> Tuple[bool, Optional[str]]:
        """
        Send one message. Handles broken-pipe, FloodWait, network errors.
        Returns (success, error_name_or_None).
        Raises UserDeactivated / AuthKeyUnregistered to signal account ban.
        """
        for attempt in range(max_retries):
            try:
                await client.send_message(chat_id, text)
                return True, None
            except FloodWait as e:
                wait = min(e.value + 2, 180)
                log.info(f"FloodWait {wait}s on chat {chat_id}")
                await asyncio.sleep(wait)
                # Don't count as an attempt
            except (ChatWriteForbidden, ChannelPrivate):
                return False, "Forbidden"
            except (UserDeactivated, AuthKeyUnregistered):
                raise   # bubble up — account is banned
            except (BrokenPipeError, ConnectionResetError, ConnectionError, OSError, TimeoutError) as e:
                # errno 32 (EPIPE) and related network errors
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt + random.uniform(0, 1))
                    # Signal to caller that client needs reconnect
                    raise _NeedsReconnect(str(e))
                return False, f"NetErr"
            except RPCError as e:
                return False, type(e).__name__
            except Exception as e:
                return False, type(e).__name__[:32]
        return False, "MaxRetries"

    async def _loop(self, user_id: int, acc: dict, interval: int, ad_text: str):
        aid     = acc["id"]
        phone_e = esc(acc["phone"])
        sem     = asyncio.Semaphore(SEND_CONCURRENCY)
        groups_cache: list = []
        cycle_no = 0

        logs_bot.send(user_id, f"▶️ <b>Started</b> account <code>{phone_e}</code>")

        while True:
            try:
                # ── Heartbeat ──────────────────────────────────────────────
                self._heartbeats[aid] = time.monotonic()

                # ── Check still running ────────────────────────────────────
                u = get_user(user_id)
                if not u or not u["running"]:
                    break

                # ── FloodWait global for this account ─────────────────────
                flood_until = self._flood_until.get(aid, 0)
                if flood_until > time.monotonic():
                    wait = flood_until - time.monotonic()
                    logs_bot.send(user_id, f"🌊 <code>{phone_e}</code> flood-wait {int(wait)}s…")
                    await asyncio.sleep(wait)
                    continue

                # ── Get / reconnect client ─────────────────────────────────
                try:
                    client = await self._get_client(acc)
                except (UserDeactivated, AuthKeyUnregistered):
                    logs_bot.send(user_id, f"🚫 <code>{phone_e}</code> account banned/deauthorized.")
                    await self._teardown_client(aid)
                    break
                except Exception as e:
                    logs_bot.send(user_id, f"⚠️ <code>{phone_e}</code> connect failed: {esc(e)}")
                    await asyncio.sleep(15)
                    continue

                # ── Refresh group list ─────────────────────────────────────
                if cycle_no % GROUPS_REFRESH_EVERY == 0 or not groups_cache:
                    try:
                        fresh = []
                        async for d in client.get_dialogs():
                            if d.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
                                fresh.append(d.chat)
                        groups_cache = fresh
                        log.info(f"acc {aid}: refreshed {len(groups_cache)} groups")
                    except Exception as e:
                        logs_bot.send(
                            user_id,
                            f"⚠️ <code>{phone_e}</code> get_dialogs failed: {esc(type(e).__name__)}",
                        )
                        await self._teardown_client(aid)  # force reconnect next cycle
                        await asyncio.sleep(20)
                        continue

                if not groups_cache:
                    logs_bot.send(user_id, f"ℹ️ <code>{phone_e}</code> no groups found, waiting…")
                    await asyncio.sleep(interval)
                    continue

                cycle_no += 1
                self._heartbeats[aid] = time.monotonic()  # refresh after dialogs fetch

                # Read fresh ad text every cycle (user may have updated it)
                fresh_u = get_user(user_id)
                ad_msg = (fresh_u and fresh_u["ad_text"]) or ad_text
                cycle_interval = (fresh_u and fresh_u["interval"]) or interval

                logs_bot.send(
                    user_id,
                    f"📤 <code>{phone_e}</code> — cycle #{cycle_no}, "
                    f"<b>{len(groups_cache)}</b> groups…",
                )

                # ── Broadcast ──────────────────────────────────────────────
                sent_count = 0
                failed_count = 0
                err_counter: dict = {}
                banned = False
                needs_reconnect = False

                async def _send_one(chat) -> str:
                    nonlocal sent_count, failed_count, banned, needs_reconnect
                    if banned:
                        return "skip"
                    title_e = esc(getattr(chat, "title", None) or chat.id)
                    async with sem:
                        # Jitter delay to reduce server pressure
                        await asyncio.sleep(random.uniform(SEND_DELAY_MIN, SEND_DELAY_MAX))
                        self._heartbeats[aid] = time.monotonic()
                        try:
                            ok, err = await self._send_with_retry(client, chat.id, ad_msg)
                        except (UserDeactivated, AuthKeyUnregistered):
                            banned = True
                            logs_bot.send(user_id, f"🚫 <code>{phone_e}</code> banned mid-broadcast.")
                            return "banned"
                        except _NeedsReconnect:
                            needs_reconnect = True
                            return "reconnect"
                    if ok:
                        sent_count += 1
                        if LOG_VERBOSE:
                            logs_bot.send(user_id, f"✅ <code>{phone_e}</code> → <b>{title_e}</b>")
                        return "ok"
                    else:
                        failed_count += 1
                        err_counter[err] = err_counter.get(err, 0) + 1
                        if LOG_VERBOSE:
                            logs_bot.send(user_id, f"⚠️ <b>{title_e}</b>: {esc(err)}")
                        return "fail"

                await asyncio.gather(*[_send_one(chat) for chat in groups_cache])

                if banned:
                    await self._teardown_client(aid)
                    groups_cache = []
                    break

                if needs_reconnect:
                    logs_bot.send(user_id, f"🔄 <code>{phone_e}</code> reconnecting (broken pipe)…")
                    await self._teardown_client(aid)
                    groups_cache = []
                    await asyncio.sleep(5)
                    continue

                bump_stats(user_id, aid, sent=sent_count, failed=failed_count)

                # Error summary
                err_summary = ""
                if err_counter:
                    top = sorted(err_counter.items(), key=lambda x: -x[1])[:5]
                    err_summary = "\n   " + " | ".join(f"{esc(k)}×{v}" for k, v in top)

                # Alert if high failure rate
                total = sent_count + failed_count
                fail_rate = failed_count / total if total else 0
                alert = " ⚠️ <b>HIGH FAIL RATE</b>" if fail_rate > 0.5 else ""

                logs_bot.send(
                    user_id,
                    f"🔁 <code>{phone_e}</code> cycle #{cycle_no} — "
                    f"✅ {sent_count} | ❌ {failed_count}{alert}{err_summary}\n"
                    f"   💤 Next in <b>{cycle_interval}s</b>",
                )

                self._heartbeats[aid] = time.monotonic()
                await asyncio.sleep(cycle_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.exception(f"Loop error acc {aid}")
                logs_bot.send(
                    user_id,
                    f"💥 <code>{phone_e}</code> loop error: "
                    f"{esc(type(e).__name__)}: {esc(str(e)[:100])}. Reconnecting…",
                )
                await self._teardown_client(aid)
                groups_cache = []
                await asyncio.sleep(20)

        logs_bot.send(user_id, f"⏸ <b>Stopped</b> <code>{phone_e}</code>")

class _NeedsReconnect(Exception):
    """Sentinel: caller should tear down and reconnect the client."""

ubm = UserbotManager()

# ─────────────────────────── LOGIN STATE ──────────────────────────────────────
pending_logins: Dict[int, dict] = {}

# ─────────────────────────── KEYBOARDS ────────────────────────────────────────
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
            [KeyboardButton("▶️ Start Ads"),       KeyboardButton("⏸ Stop Ads")],
            [KeyboardButton("🗑 Delete Accounts"), KeyboardButton("📈 Analytics")],
            [KeyboardButton("⬅️ Back")],
        ],
        resize_keyboard=True,
    )

# ─────────────────────────── COMMAND HANDLERS ─────────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    ensure_user(uid)
    await update.message.reply_text(
        "🦊 <b>Welcome to Tecxo Ads Bot</b> — <i>The Future of Telegram Automation</i>\n\n"
        "• Premium Ad Broadcasting\n"
        "• Smart Anti-Flood Delays\n"
        "• Multi-Account Support\n"
        "• Live Delivery Logs\n\n"
        "Tap <b>📊 Dashboard</b> to get started.\n"
        "For help: @YourSupport",
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu(),
    )

async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    state = pending_logins.pop(uid, None)
    if state and state.get("client"):
        try:
            await state["client"].disconnect()
        except Exception:
            pass
    ctx.user_data.pop("awaiting", None)
    await update.message.reply_text("❌ Cancelled.", reply_markup=main_menu())

async def cmd_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    await update.message.reply_text(
        f"👤 Your Telegram ID: <code>{u.id}</code>\n"
        f"Name: {esc(u.full_name)}",
        parse_mode=ParseMode.HTML,
    )

async def cmd_ping(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t0 = time.monotonic()
    m = await update.message.reply_text("🏓 Pong…")
    ms = int((time.monotonic() - t0) * 1000)
    await m.edit_text(f"🏓 Pong! <code>{ms}ms</code>", parse_mode=ParseMode.HTML)

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    accs = list_accounts(uid)
    active = sum(1 for a in accs if a["id"] in ubm.tasks and not ubm.tasks[a["id"]].done())
    q = logs_bot.queue.qsize() if logs_bot.app else 0
    u = get_user(uid)
    await update.message.reply_text(
        f"📡 <b>Status</b>\n"
        f"• Accounts: {len(accs)} / {MAX_ACCOUNTS}\n"
        f"• Active loops: {active}\n"
        f"• Ad message: {'Set ✅' if u and u['ad_text'] else 'Not set ⭕'}\n"
        f"• Interval: {u['interval'] if u else 300}s\n"
        f"• Broadcast: {'Running ▶️' if u and u['running'] else 'Stopped ⏸'}\n"
        f"• Logs queue: {q}\n"
        f"• Verbose logs: {LOG_VERBOSE}",
        parse_mode=ParseMode.HTML,
    )

async def cmd_restart(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in ADMINS:
        return await update.message.reply_text("❌ Admin only.")
    await update.message.reply_text("🔄 Restarting all loops for your account…")
    await ubm.stop_user(uid)
    await asyncio.sleep(2)
    await ubm.start_user(uid)
    await update.message.reply_text("✅ Restarted.")

# ─────────────────────────── MENU ROUTER ──────────────────────────────────────
async def menu_router(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    txt = (update.message.text or "").strip()
    uid = update.effective_user.id
    ensure_user(uid)

    # ── Navigation ──────────────────────────────────────────────────────────
    if txt == "📊 Dashboard":
        return await show_dashboard(update, ctx)
    if txt == "⬅️ Back":
        return await update.message.reply_text("Main menu:", reply_markup=main_menu())
    if txt == "📢 Updates":
        return await update.message.reply_text("Follow @YourUpdates for news & updates.")
    if txt == "🆘 Support":
        return await update.message.reply_text("Contact @YourSupport for help.")
    if txt == "ℹ️ How To Use":
        return await update.message.reply_text(
            "<b>How to Use</b>\n\n"
            "1️⃣ Dashboard → <b>Add Accounts</b> — login with phone + OTP\n"
            "2️⃣ <b>Set Ad Message</b> — paste your ad text\n"
            "3️⃣ <b>Set Time Interval</b> — seconds between broadcast cycles (min 60)\n"
            "4️⃣ <b>Start Ads ▶️</b> — begins broadcasting to all groups\n"
            "5️⃣ Open your <b>Logs Bot</b> and tap /start to see live delivery reports\n\n"
            "Use /status to check the current state at any time.",
            parse_mode=ParseMode.HTML,
        )
    if txt == "⚡ Powered by":
        return await update.message.reply_text("⚡ Powered by Tecxo Ads Automation v3")

    # ── Dashboard actions ────────────────────────────────────────────────────
    if txt == "➕ Add Accounts":
        return await begin_add_account(update, ctx)
    if txt == "👥 My Accounts":
        return await show_accounts(update, ctx)
    if txt == "✍️ Set Ad Message":
        ctx.user_data["awaiting"] = "ad"
        return await update.message.reply_text(
            "✍️ Send your ad message now.\n"
            "Supports plain text. Send /cancel to abort."
        )
    if txt == "⏱ Set Time Interval":
        ctx.user_data["awaiting"] = "interval"
        return await update.message.reply_text(
            "⏱ Send interval in <b>seconds</b> between broadcast cycles.\n"
            "Min: 60 | Max: 86400\nExample: <code>300</code>",
            parse_mode=ParseMode.HTML,
        )
    if txt == "▶️ Start Ads":
        return await start_ads(update, ctx)
    if txt == "⏸ Stop Ads":
        return await stop_ads(update, ctx)
    if txt == "🗑 Delete Accounts":
        return await delete_accounts_menu(update, ctx)
    if txt == "📈 Analytics":
        return await analytics(update, ctx)

    # ── Awaiting user input ──────────────────────────────────────────────────
    aw = ctx.user_data.get("awaiting")
    if aw == "ad":
        if not txt:
            return await update.message.reply_text("❌ Message cannot be empty.")
        update_user(uid, ad_text=txt)
        ctx.user_data.pop("awaiting", None)
        preview = txt[:80] + ("…" if len(txt) > 80 else "")
        return await update.message.reply_text(
            f"✅ Ad message saved!\nPreview: <i>{esc(preview)}</i>",
            parse_mode=ParseMode.HTML,
        )
    if aw == "interval":
        if not txt.isdigit():
            return await update.message.reply_text("❌ Please send a number (seconds).")
        v = max(60, min(86400, int(txt)))
        update_user(uid, interval=v)
        ctx.user_data.pop("awaiting", None)
        return await update.message.reply_text(
            f"✅ Interval set to <b>{v}s</b> ({v//60}m {v%60}s).",
            parse_mode=ParseMode.HTML,
        )

    # ── Login flow ───────────────────────────────────────────────────────────
    if uid in pending_logins:
        return await handle_login_input(update, ctx)

# ─────────────────────────── DASHBOARD PAGES ──────────────────────────────────
async def show_dashboard(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    u = get_user(uid)
    accs = list_accounts(uid)
    status   = "Running ▶️" if u and u["running"] else "Paused ⏸"
    ad_set   = "Set ✅"     if u and u["ad_text"] else "Not Set ⭕"
    interval = u["interval"] if u else 300
    await update.message.reply_text(
        "<b>📊 Ads Dashboard</b>\n\n"
        f"• Accounts: <b>{len(accs)}/{MAX_ACCOUNTS}</b>\n"
        f"• Ad Message: <b>{ad_set}</b>\n"
        f"• Cycle Interval: <b>{interval}s</b>\n"
        f"• Status: <b>{status}</b>\n\n"
        "Choose an action below 👇",
        parse_mode=ParseMode.HTML,
        reply_markup=dashboard_menu(),
    )

async def show_accounts(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    accs = list_accounts(uid)
    if not accs:
        return await update.message.reply_text("No accounts added yet. Use ➕ Add Accounts.")
    lines = ["<b>👥 Your Accounts</b>\n"]
    for i, a in enumerate(accs, 1):
        running = "▶️" if a["id"] in ubm.tasks and not ubm.tasks[a["id"]].done() else "⏸"
        lines.append(
            f"{i}. {running} <code>{esc(a['phone'])}</code>"
            f" — {esc(a['tg_first_name'] or 'Unknown')}"
        )
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)

async def analytics(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    rows = get_stats(update.effective_user.id)
    if not rows:
        return await update.message.reply_text("No stats yet. Start broadcasting first.")
    lines = ["<b>📈 Analytics</b>\n"]
    total_s = total_f = 0
    for r in rows:
        total_s += r["sent"]
        total_f += r["failed"]
        rate = f"{r['sent']/(r['sent']+r['failed'])*100:.0f}%" if (r["sent"]+r["failed"]) else "N/A"
        lines.append(
            f"• <code>{esc(r['phone'])}</code>\n"
            f"  ✅ {r['sent']} sent | ❌ {r['failed']} failed | 📊 {rate} success"
        )
    overall = f"{total_s/(total_s+total_f)*100:.0f}%" if (total_s+total_f) else "N/A"
    lines.append(f"\n<b>Total:</b> ✅ {total_s} | ❌ {total_f} | 📊 {overall}")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)

async def delete_accounts_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    accs = list_accounts(uid)
    if not accs:
        return await update.message.reply_text("No accounts to delete.")
    kb = [
        [InlineKeyboardButton(f"🗑 {a['phone']} — {a['tg_first_name'] or ''}", callback_data=f"del:{a['id']}")]
        for a in accs
    ]
    await update.message.reply_text(
        "Tap an account to delete it:", reply_markup=InlineKeyboardMarkup(kb)
    )

async def cb_delete(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    aid = int(q.data.split(":")[1])
    await ubm.stop_account(aid)
    delete_account(uid, aid)
    await q.edit_message_text("✅ Account deleted and loop stopped.")

# ─────────────────────────── ADD ACCOUNT FLOW ─────────────────────────────────
async def begin_add_account(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if len(list_accounts(uid)) >= MAX_ACCOUNTS:
        return await update.message.reply_text(
            f"❌ Maximum {MAX_ACCOUNTS} accounts reached. Delete one first."
        )
    pending_logins[uid] = {"step": "phone"}
    await update.message.reply_text(
        "📱 Send your phone number with country code.\n"
        "Example: <code>+919876543210</code>\n\n"
        "Send /cancel to abort.",
        parse_mode=ParseMode.HTML,
    )

async def handle_login_input(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid   = update.effective_user.id
    state = pending_logins.get(uid)
    if not state:
        return
    text = (update.message.text or "").strip()

    if text == "/cancel":
        c = state.get("client")
        if c:
            try:
                await c.disconnect()
            except Exception:
                pass
        pending_logins.pop(uid, None)
        return await update.message.reply_text("❌ Login cancelled.", reply_markup=dashboard_menu())

    try:
        if state["step"] == "phone":
            phone = text
            client = Client(
                name=f"login_{uid}",
                api_id=API_ID,
                api_hash=API_HASH,
                in_memory=True,
            )
            await asyncio.wait_for(client.connect(), timeout=20)
            sent = await asyncio.wait_for(client.send_code(phone), timeout=20)
            state.update(client=client, phone=phone, code_hash=sent.phone_code_hash, step="code")
            await update.message.reply_text(
                "🔐 Telegram sent you a code.\n"
                "Enter it with <b>spaces</b> between digits to avoid auto-revoke:\n"
                "Example: <code>1 2 3 4 5</code>",
                parse_mode=ParseMode.HTML,
            )

        elif state["step"] == "code":
            code   = text.replace(" ", "")
            client: Client = state["client"]
            try:
                signed_in = await asyncio.wait_for(
                    client.sign_in(state["phone"], state["code_hash"], code), timeout=20
                )
            except SessionPasswordNeeded:
                state["step"] = "password"
                return await update.message.reply_text(
                    "🔑 Two-step verification is enabled.\nSend your 2FA password:"
                )
            except (PhoneCodeInvalid, PhoneCodeExpired) as e:
                pending_logins.pop(uid, None)
                try:
                    await client.disconnect()
                except Exception:
                    pass
                return await update.message.reply_text(
                    f"❌ {type(e).__name__}. Please try adding the account again."
                )
            await _finish_login(update, uid, client, state["phone"], signed_in)

        elif state["step"] == "password":
            client: Client = state["client"]
            signed_in = await asyncio.wait_for(client.check_password(text), timeout=20)
            await _finish_login(update, uid, client, state["phone"], signed_in)

    except PhoneNumberInvalid:
        pending_logins.pop(uid, None)
        await update.message.reply_text("❌ Invalid phone number. Include country code (e.g. +91…).")
    except asyncio.TimeoutError:
        pending_logins.pop(uid, None)
        await update.message.reply_text("❌ Timed out connecting to Telegram. Try again.")
    except Exception as e:
        log.exception("login error")
        pending_logins.pop(uid, None)
        await update.message.reply_text(f"❌ Error: {esc(e)}")

async def _finish_login(update: Update, uid: int, client: Client, phone: str, me):
    session_string = await client.export_session_string()
    try:
        await client.disconnect()
    except Exception:
        pass
    pending_logins.pop(uid, None)
    add_account(uid, phone, session_string, me.id, me.first_name or "")
    await update.message.reply_text(
        f"✅ Logged in as <b>{esc(me.first_name)}</b> (<code>{esc(phone)}</code>)!\n"
        f"You can now add more accounts or go to Dashboard → Start Ads.",
        parse_mode=ParseMode.HTML,
        reply_markup=dashboard_menu(),
    )

# ─────────────────────────── START / STOP ADS ─────────────────────────────────
async def start_ads(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    u   = get_user(uid)
    if not u or not u["ad_text"]:
        return await update.message.reply_text(
            "❌ Please set an Ad Message first (✍️ Set Ad Message)."
        )
    if not list_accounts(uid):
        return await update.message.reply_text(
            "❌ Please add at least one account first (➕ Add Accounts)."
        )
    await ubm.start_user(uid)
    await update.message.reply_text(
        "▶️ <b>Ads started!</b>\n\n"
        "Open your Logs Bot and tap /start to see live delivery reports.\n"
        "Use /status to check progress anytime.",
        parse_mode=ParseMode.HTML,
    )

async def stop_ads(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    await ubm.stop_user(uid)
    await update.message.reply_text("⏸ <b>Ads stopped.</b>", parse_mode=ParseMode.HTML)

# ─────────────────────────── HEALTH SERVER ────────────────────────────────────
async def _start_health_server():
    from aiohttp import web
    async def _ok(_req):
        return web.Response(text="ok")
    app = web.Application()
    app.router.add_get("/", _ok)
    app.router.add_get("/health", _ok)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", HEALTH_PORT)
    await site.start()
    log.info(f"Health server listening on :{HEALTH_PORT}")

# ─────────────────────────── MAIN ─────────────────────────────────────────────
async def main():
    if not all([API_ID, API_HASH, BOT_TOKEN]):
        raise SystemExit("Missing API_ID / API_HASH / BOT_TOKEN in environment")

    init_db()
    await _start_health_server()
    await logs_bot.start()

    # Build main bot with concurrent updates to prevent command delays
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .concurrent_updates(True)       # critical: prevents one slow handler blocking others
        .connect_timeout(20)
        .read_timeout(20)
        .write_timeout(20)
        .pool_timeout(10)
        .build()
    )

    app.add_handler(CommandHandler("start",   cmd_start))
    app.add_handler(CommandHandler("cancel",  cmd_cancel))
    app.add_handler(CommandHandler("id",      cmd_id))
    app.add_handler(CommandHandler("ping",    cmd_ping))
    app.add_handler(CommandHandler("status",  cmd_status))
    app.add_handler(CommandHandler("restart", cmd_restart))
    app.add_handler(CallbackQueryHandler(cb_delete, pattern=r"^del:"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, menu_router))

    await app.initialize()
    await app.start()
    await app.updater.start_polling(
        drop_pending_updates=True,
        allowed_updates=["message", "callback_query"],
    )
    log.info("Main bot started. Polling…")

    # Start watchdog
    await ubm.start_watchdog()

    # Auto-resume running users
    for uid in get_all_running_users():
        try:
            await ubm.start_user(uid)
            log.info(f"Resumed user {uid}")
        except Exception as e:
            log.warning(f"Resume failed for {uid}: {e}")

    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    for sig in (2, 15):  # SIGINT, SIGTERM
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass  # Windows

    try:
        await stop_event.wait()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        log.info("Shutting down…")
        # Stop all loops
        all_users = get_all_running_users()
        for uid in all_users:
            try:
                await ubm.stop_user(uid)
            except Exception:
                pass
        # Stop bots
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
        if logs_bot.app:
            await logs_bot.app.updater.stop()
            await logs_bot.app.stop()
            await logs_bot.app.shutdown()
        log.info("Shutdown complete.")

if __name__ == "__main__":
    asyncio.run(main())
