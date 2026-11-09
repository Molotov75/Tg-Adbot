# ============================================================
# LazAdsBot — Telegram Userbot Broadcaster
# Powered by @TechnicalDevs
# Bot: @LazAdsbot
# ============================================================

from flask import Flask
from threading import Thread

app = Flask(__name__)

@app.route("/")
def home():
    return "LazAdsBot 🚀 is running!", 200

@app.route("/health")
def health():
    return "OK", 200

def run():
    app.run(host="0.0.0.0", port=10000)

def keep_alive():
    server = Thread(target=run, daemon=True)
    server.start()

keep_alive()

# ── stdlib ────────────────────────────────────────────────────────────────────
import asyncio
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

# ── aiogram ───────────────────────────────────────────────────────────────────
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)

# ── telethon (userbot) ────────────────────────────────────────────────────────
try:
    from telethon import TelegramClient, errors as tl_errors
    from telethon.sessions import StringSession
    TELETHON_OK = True
except ImportError:
    TELETHON_OK = False
    print("WARNING: telethon not installed. Userbot features disabled.")

# ── SQLAlchemy async ──────────────────────────────────────────────────────────
import ssl
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import (
    BigInteger, Boolean, DateTime, Integer, String, Text, func, select, update
)
from sqlalchemy.pool import NullPool

log = logging.getLogger(__name__)

# ═════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ═════════════════════════════════════════════════════════════════════════════

BOT_TOKEN      = "9214581560:AAGG5GC7KyalFaSs6tt62yZm9uknmzgYITs"
ADMIN_IDS      = [8404630327]
BOT_USERNAME   = "LazAdsbot"
POWERED_BY     = "@TechnicalDevs"
MUST_JOIN      = "@TechnicalDevs"           # channel users must join
DATABASE_URL   = (
    "postgresql+asyncpg://neondb_owner:npg_J2ZtUBWYxz1q"
    "@ep-green-dew-bd44ci17-pooler.c-8.us-east-2.aws.neon.tech/neondb"
)

# Telegram API credentials for userbot (get from my.telegram.org)
# These are stored per-user in the DB; bot uses its own TG app credentials
TG_API_ID      = int(os.getenv("TG_API_ID", "2040"))        # fallback = TDesktop
TG_API_HASH    = os.getenv("TG_API_HASH", "b18441a1ff607e10a989891a5462e627")

MAX_FREE_ACCOUNTS  = 2
MAX_PRO_ACCOUNTS   = 10
MAX_FREE_GROUPS    = 20

AD_SUFFIX_DEFAULT  = f"~ @{BOT_USERNAME}"   # appended to every ad message

PLAN_PRICES = {
    "pro":      {"label": "Pro 👑",      "price": "₹99/month",  "accounts": 10, "interval_min": 60},
    "business": {"label": "Business 💎", "price": "₹299/month", "accounts": 20, "interval_min": 30},
}

# ═════════════════════════════════════════════════════════════════════════════
#  DATABASE
# ═════════════════════════════════════════════════════════════════════════════

ssl_ctx = ssl.create_default_context()

engine = create_async_engine(
    DATABASE_URL,
    poolclass=NullPool,
    connect_args={"ssl": ssl_ctx},
)

AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class LazUser(Base):
    __tablename__ = "laz_users"

    id              : Mapped[int]  = mapped_column(BigInteger, primary_key=True)   # TG user id
    username        : Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    full_name       : Mapped[str]  = mapped_column(String(256), default="")
    plan            : Mapped[str]  = mapped_column(String(32), default="free")
    plan_expires    : Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    joined_channel  : Mapped[bool] = mapped_column(Boolean, default=False)
    created_at      : Mapped[datetime] = mapped_column(DateTime, default=func.now())


class HostedAccount(Base):
    __tablename__ = "laz_accounts"

    id              : Mapped[int]  = mapped_column(Integer, primary_key=True, autoincrement=True)
    owner_id        : Mapped[int]  = mapped_column(BigInteger, index=True)
    phone           : Mapped[str]  = mapped_column(String(32))
    session_string  : Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    tg_user_id      : Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    tg_username     : Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    is_active       : Mapped[bool] = mapped_column(Boolean, default=False)
    broadcast_running: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at      : Mapped[datetime] = mapped_column(DateTime, default=func.now())


class BroadcastConfig(Base):
    __tablename__ = "laz_broadcast_config"

    id              : Mapped[int]  = mapped_column(Integer, primary_key=True, autoincrement=True)
    owner_id        : Mapped[int]  = mapped_column(BigInteger, unique=True, index=True)
    ad_message      : Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ad_suffix       : Mapped[str]  = mapped_column(String(128), default=AD_SUFFIX_DEFAULT)
    interval_seconds: Mapped[int]  = mapped_column(Integer, default=300)
    delay_seconds   : Mapped[int]  = mapped_column(Integer, default=5)
    auto_reply_msg  : Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    auto_reply_on   : Mapped[bool] = mapped_column(Boolean, default=False)
    auto_join_on    : Mapped[bool] = mapped_column(Boolean, default=False)
    service_type    : Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    target_groups   : Mapped[Optional[str]] = mapped_column(Text, nullable=True)   # JSON list
    updated_at      : Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())


class BotSettings(Base):
    """Admin-customisable global settings."""
    __tablename__ = "laz_bot_settings"

    key   : Mapped[str] = mapped_column(String(64), primary_key=True)
    value : Mapped[str] = mapped_column(Text, default="")


async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    log.info("LazAdsBot DB ready.")


# ─── helpers ──────────────────────────────────────────────────────────────────

async def get_or_create_user(session: AsyncSession, tg_user) -> LazUser:
    result = await session.execute(select(LazUser).where(LazUser.id == tg_user.id))
    user = result.scalar_one_or_none()
    if not user:
        user = LazUser(
            id=tg_user.id,
            username=tg_user.username,
            full_name=tg_user.full_name,
        )
        session.add(user)
        await session.commit()
    return user


async def get_or_create_config(session: AsyncSession, owner_id: int) -> BroadcastConfig:
    result = await session.execute(
        select(BroadcastConfig).where(BroadcastConfig.owner_id == owner_id)
    )
    cfg = result.scalar_one_or_none()
    if not cfg:
        cfg = BroadcastConfig(owner_id=owner_id)
        session.add(cfg)
        await session.commit()
    return cfg


async def get_setting(session: AsyncSession, key: str, default: str = "") -> str:
    result = await session.execute(select(BotSettings).where(BotSettings.key == key))
    row = result.scalar_one_or_none()
    return row.value if row else default


async def set_setting(session: AsyncSession, key: str, value: str) -> None:
    result = await session.execute(select(BotSettings).where(BotSettings.key == key))
    row = result.scalar_one_or_none()
    if row:
        row.value = value
    else:
        session.add(BotSettings(key=key, value=value))
    await session.commit()


async def count_accounts(session: AsyncSession, owner_id: int) -> int:
    result = await session.execute(
        select(func.count()).where(HostedAccount.owner_id == owner_id)
    )
    return result.scalar_one()


async def get_accounts(session: AsyncSession, owner_id: int) -> List[HostedAccount]:
    result = await session.execute(
        select(HostedAccount).where(HostedAccount.owner_id == owner_id)
    )
    return list(result.scalars().all())


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def plan_account_limit(plan: str) -> int:
    if plan == "pro":
        return PLAN_PRICES["pro"]["accounts"]
    if plan == "business":
        return PLAN_PRICES["business"]["accounts"]
    return MAX_FREE_ACCOUNTS


def plan_interval_min(plan: str) -> int:
    if plan == "pro":
        return PLAN_PRICES["pro"]["interval_min"]
    if plan == "business":
        return PLAN_PRICES["business"]["interval_min"]
    return 300  # free: 5 min min


# ═════════════════════════════════════════════════════════════════════════════
#  KEYBOARDS
# ═════════════════════════════════════════════════════════════════════════════

def kb_main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Add account", callback_data="add_account")],
        [
            InlineKeyboardButton(text="📢 Set Advertisement", callback_data="set_ad"),
            InlineKeyboardButton(text="⏱ Interval & delay", callback_data="set_interval"),
        ],
        [InlineKeyboardButton(text="▶️ Run Ads", callback_data="run_ads")],
        [
            InlineKeyboardButton(text="💬 Auto reply", callback_data="auto_reply"),
            InlineKeyboardButton(text="🔗 Auto join", callback_data="auto_join"),
        ],
        [InlineKeyboardButton(text="💎 Go Premium", callback_data="premium")],
        [
            InlineKeyboardButton(text="ℹ️ About bot", url=f"https://t.me/{BOT_USERNAME}"),
            InlineKeyboardButton(text="⚡ Powered by", url=f"https://t.me/TechnicalDevs"),
        ],
    ])


def kb_otp_pad() -> InlineKeyboardMarkup:
    rows = []
    for row in [["1","2","3"],["4","5","6"],["7","8","9"]]:
        rows.append([InlineKeyboardButton(text=d, callback_data=f"otp_digit:{d}") for d in row])
    rows.append([
        InlineKeyboardButton(text="⌫", callback_data="otp_digit:DEL"),
        InlineKeyboardButton(text="0", callback_data="otp_digit:0"),
        InlineKeyboardButton(text="❌ Cancel", callback_data="otp_cancel"),
    ])
    rows.append([InlineKeyboardButton(text="✅ Show Code", callback_data="otp_show")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_interval() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🟢 20min", callback_data="interval:1200"),
            InlineKeyboardButton(text="🔴 5min",  callback_data="interval:300"),
            InlineKeyboardButton(text="🟡 10min", callback_data="interval:600"),
        ],
        [InlineKeyboardButton(text="🔙 Back", callback_data="dashboard")],
    ])


def kb_auto_reply(active: bool) -> InlineKeyboardMarkup:
    toggle = "⏹ Deactivate" if active else "▶️ Activate Auto reply"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=toggle, callback_data="toggle_auto_reply")],
        [InlineKeyboardButton(text="🔙 Back", callback_data="dashboard")],
    ])


def kb_back(cb: str = "dashboard") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Back", callback_data=cb)]
    ])


def kb_admin_panel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Edit bot name/suffix", callback_data="admin_edit_name")],
        [InlineKeyboardButton(text="📣 Broadcast to all users", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="👑 Grant Premium", callback_data="admin_grant_premium")],
        [InlineKeyboardButton(text="🚫 Ban user", callback_data="admin_ban")],
        [InlineKeyboardButton(text="📊 Stats", callback_data="admin_stats")],
        [InlineKeyboardButton(text="🔙 Back", callback_data="dashboard")],
    ])


# ═════════════════════════════════════════════════════════════════════════════
#  FSM STATES
# ═════════════════════════════════════════════════════════════════════════════

class AddAccount(StatesGroup):
    waiting_phone   = State()
    waiting_otp     = State()
    waiting_2fa     = State()


class SetAd(StatesGroup):
    waiting_message = State()


class SetInterval(StatesGroup):
    waiting_custom  = State()


class SetAutoReply(StatesGroup):
    waiting_message = State()


class AdminStates(StatesGroup):
    broadcast_msg   = State()
    grant_user_id   = State()
    ban_user_id     = State()
    edit_name       = State()


# ═════════════════════════════════════════════════════════════════════════════
#  GLOBAL: active Telethon clients & broadcast tasks
# ═════════════════════════════════════════════════════════════════════════════

active_clients: Dict[int, Any] = {}     # account_id → TelegramClient
broadcast_tasks: Dict[int, asyncio.Task] = {}   # owner_id → Task
otp_buffers: Dict[int, str] = {}        # user_id → digits typed so far
pending_clients: Dict[int, Any] = {}    # user_id → TelegramClient (mid-login)

bot_instance: Optional[Bot] = None


# ═════════════════════════════════════════════════════════════════════════════
#  FORCE-JOIN CHECK
# ═════════════════════════════════════════════════════════════════════════════

async def check_joined(bot: Bot, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=MUST_JOIN, user_id=user_id)
        return member.status not in ("left", "kicked", "banned")
    except Exception:
        return False   # channel not found / bot not admin → let through


# ═════════════════════════════════════════════════════════════════════════════
#  DASHBOARD builder
# ═════════════════════════════════════════════════════════════════════════════

DASHBOARD_PHOTO = "https://i.imgur.com/PLACEHOLDER.jpg"   # replace with your banner

async def send_dashboard(message: Message, user: LazUser, session: AsyncSession,
                          bot_name: str) -> None:
    cfg   = await get_or_create_config(session, user.id)
    accts = await get_accounts(session, user.id)
    limit = plan_account_limit(user.plan)

    running = any(a.broadcast_running for a in accts)
    status  = "🟢 running" if running else "🔴 stopped"
    service = cfg.service_type or "Not set"
    plan_label = user.plan.capitalize()

    text = (
        f"━━━━ Powered by {POWERED_BY} ━━━━\n\n"
        f"• Hosted Accounts: <b>{len(accts)}/{limit}</b>\n"
        f"• Service: <i>{service}</i>\n"
        f"• Advertisement status: <i>{status}</i>\n"
        f"{'🟢' if running else '🔴'}\n"
        f"• Interval: <i>{cfg.interval_seconds // 60} minutes</i>\n"
        f"• Current plan: <i>{plan_label}</i>\n\n"
        f"<blockquote>Developed and managed by:\n{POWERED_BY}</blockquote>"
    )
    try:
        await message.answer_photo(
            photo=DASHBOARD_PHOTO,
            caption=text,
            reply_markup=kb_main_menu(),
        )
    except Exception:
        await message.answer(text, reply_markup=kb_main_menu())


# ═════════════════════════════════════════════════════════════════════════════
#  ROUTER
# ═════════════════════════════════════════════════════════════════════════════

router = Router()


# ─── /start ───────────────────────────────────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext) -> None:
    await state.clear()
    bot  = message.bot
    uid  = message.from_user.id

    # force-join check
    joined = await check_joined(bot, uid)
    if not joined:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"📢 Join {MUST_JOIN}", url=f"https://t.me/{MUST_JOIN.lstrip('@')}")],
            [InlineKeyboardButton(text="✅ I joined", callback_data="check_join")],
        ])
        await message.answer(
            f"⚠️ You must join <b>{MUST_JOIN}</b> to use this bot.\n\n"
            f"After joining press <b>✅ I joined</b>.",
            reply_markup=kb,
        )
        return

    async with AsyncSessionLocal() as session:
        user     = await get_or_create_user(session, message.from_user)
        bot_name = await get_setting(session, "bot_name", f"LazAdsBot 🚀")
        await send_dashboard(message, user, session, bot_name)


@router.callback_query(F.data == "check_join")
async def cb_check_join(cb: CallbackQuery, state: FSMContext) -> None:
    joined = await check_joined(cb.bot, cb.from_user.id)
    if not joined:
        await cb.answer("❌ You haven't joined yet!", show_alert=True)
        return
    await cb.message.delete()
    async with AsyncSessionLocal() as session:
        user     = await get_or_create_user(session, cb.from_user)
        bot_name = await get_setting(session, "bot_name", "LazAdsBot 🚀")
        await send_dashboard(cb.message, user, session, bot_name)
    await cb.answer()


@router.callback_query(F.data == "dashboard")
async def cb_dashboard(cb: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await cb.message.delete()
    async with AsyncSessionLocal() as session:
        user     = await get_or_create_user(session, cb.from_user)
        bot_name = await get_setting(session, "bot_name", "LazAdsBot 🚀")
        await send_dashboard(cb.message, user, session, bot_name)
    await cb.answer()


# ─── ADD ACCOUNT ──────────────────────────────────────────────────────────────

@router.callback_query(F.data == "add_account")
async def cb_add_account(cb: CallbackQuery, state: FSMContext) -> None:
    async with AsyncSessionLocal() as session:
        user  = await get_or_create_user(session, cb.from_user)
        count = await count_accounts(session, cb.from_user.id)
        limit = plan_account_limit(user.plan)

    if count >= limit:
        await cb.answer(
            f"❌ Account limit reached ({count}/{limit}). Upgrade to Premium for more!",
            show_alert=True,
        )
        return

    await cb.message.edit_caption(
        caption=(
            "🔐 <b>HOST NEW ACCOUNT</b>\n\n"
            "<b>Secure Account Hosting</b>\n\n"
            "Enter your phone number with country code:\n\n"
            "<blockquote>Example: +1234567890</blockquote>\n\n"
            "Your data is encrypted and secure"
        ),
        reply_markup=kb_back(),
    )
    await state.set_state(AddAccount.waiting_phone)
    await cb.answer()


@router.message(AddAccount.waiting_phone)
async def handle_phone(message: Message, state: FSMContext) -> None:
    phone = message.text.strip()
    if not re.match(r"^\+\d{7,15}$", phone):
        await message.answer("❌ Invalid format. Use: +1234567890")
        return

    if not TELETHON_OK:
        await message.answer("❌ Telethon not installed. Contact admin.")
        await state.clear()
        return

    client = TelegramClient(StringSession(), TG_API_ID, TG_API_HASH)
    await client.connect()

    pending_clients[message.from_user.id] = client
    otp_buffers[message.from_user.id] = ""

    try:
        result = await client.send_code_request(phone)
        await state.update_data(phone=phone, phone_code_hash=result.phone_code_hash)
    except tl_errors.FloodWaitError as e:
        await message.answer(f"⚠️ Flood wait. Try again in {e.seconds}s.")
        await client.disconnect()
        del pending_clients[message.from_user.id]
        await state.clear()
        return
    except Exception as e:
        await message.answer(f"❌ Error: {e}")
        await client.disconnect()
        del pending_clients[message.from_user.id]
        await state.clear()
        return

    otp_msg = await message.answer(
        f"📲 <b>OTP sent to {phone}!</b> ✅\n\n"
        "Enter the OTP using the keypad below\n"
        f"<b>Current:</b> <code>_____</code>\n"
        "<b>Format:</b> 12345 (no spaces needed)\n"
        "<i>Valid for: 5 minutes</i>",
        reply_markup=kb_otp_pad(),
    )
    await state.update_data(otp_msg_id=otp_msg.message_id)
    await state.set_state(AddAccount.waiting_otp)


@router.callback_query(F.data.startswith("otp_digit:"))
async def cb_otp_digit(cb: CallbackQuery, state: FSMContext) -> None:
    digit = cb.data.split(":")[1]
    uid   = cb.from_user.id

    if digit == "DEL":
        otp_buffers[uid] = otp_buffers.get(uid, "")[:-1]
    elif digit == "DEL_ALL":
        otp_buffers[uid] = ""
    else:
        buf = otp_buffers.get(uid, "")
        if len(buf) < 5:
            otp_buffers[uid] = buf + digit

    buf = otp_buffers.get(uid, "")
    display = buf.ljust(5, "_")

    data = await state.get_data()
    phone = data.get("phone", "")

    try:
        await cb.message.edit_caption(
            caption=(
                f"📲 <b>OTP sent to {phone}!</b> ✅\n\n"
                "Enter the OTP using the keypad below\n"
                f"<b>Current:</b> <code>{display}</code>\n"
                "<b>Format:</b> 12345 (no spaces needed)\n"
                "<i>Valid for: 5 minutes</i>"
            ),
            reply_markup=kb_otp_pad(),
        )
    except Exception:
        pass

    # auto-submit when 5 digits
    if len(otp_buffers.get(uid, "")) == 5:
        await _submit_otp(cb, state)

    await cb.answer()


@router.callback_query(F.data == "otp_show")
async def cb_otp_show(cb: CallbackQuery, state: FSMContext) -> None:
    buf = otp_buffers.get(cb.from_user.id, "")
    await cb.answer(f"Code so far: {buf or '(empty)'}", show_alert=True)


@router.callback_query(F.data == "otp_cancel")
async def cb_otp_cancel(cb: CallbackQuery, state: FSMContext) -> None:
    uid = cb.from_user.id
    if uid in pending_clients:
        try:
            await pending_clients[uid].disconnect()
        except Exception:
            pass
        del pending_clients[uid]
    otp_buffers.pop(uid, None)
    await state.clear()
    await cb.message.delete()
    await cb.answer("Cancelled.")

    async with AsyncSessionLocal() as session:
        user     = await get_or_create_user(session, cb.from_user)
        bot_name = await get_setting(session, "bot_name", "LazAdsBot 🚀")
        await send_dashboard(cb.message, user, session, bot_name)


async def _submit_otp(cb: CallbackQuery, state: FSMContext) -> None:
    uid    = cb.from_user.id
    code   = otp_buffers.get(uid, "")
    data   = await state.get_data()
    phone  = data["phone"]
    pch    = data["phone_code_hash"]
    client = pending_clients.get(uid)

    if not client:
        await cb.message.answer("❌ Session expired. Please start again.")
        await state.clear()
        return

    try:
        me = await client.sign_in(phone=phone, code=code, phone_code_hash=pch)
    except tl_errors.SessionPasswordNeededError:
        await cb.message.edit_caption(
            caption="🔐 2FA is enabled. Enter your password:",
            reply_markup=kb_back("dashboard"),
        )
        await state.set_state(AddAccount.waiting_2fa)
        return
    except tl_errors.PhoneCodeInvalidError:
        otp_buffers[uid] = ""
        try:
            await cb.message.edit_caption(
                caption=(
                    f"❌ <b>Wrong OTP!</b> Try again.\n\n"
                    "Enter the OTP using the keypad below\n"
                    f"<b>Current:</b> <code>_____</code>"
                ),
                reply_markup=kb_otp_pad(),
            )
        except Exception:
            pass
        return
    except Exception as e:
        await cb.message.answer(f"❌ Sign-in error: {e}")
        await state.clear()
        return

    await _save_account(cb, state, client, me, phone)


@router.message(AddAccount.waiting_2fa)
async def handle_2fa(message: Message, state: FSMContext) -> None:
    uid    = message.from_user.id
    client = pending_clients.get(uid)
    if not client:
        await message.answer("❌ Session expired.")
        await state.clear()
        return

    try:
        me = await client.sign_in(password=message.text.strip())
    except tl_errors.PasswordHashInvalidError:
        await message.answer("❌ Wrong password. Try again:")
        return
    except Exception as e:
        await message.answer(f"❌ Error: {e}")
        await state.clear()
        return

    data  = await state.get_data()
    phone = data.get("phone", "")
    await _save_account(message, state, client, me, phone)


async def _save_account(event, state: FSMContext, client, me, phone: str) -> None:
    uid       = event.from_user.id
    session_s = client.session.save()

    async with AsyncSessionLocal() as session:
        acct = HostedAccount(
            owner_id=uid,
            phone=phone,
            session_string=session_s,
            tg_user_id=me.id,
            tg_username=getattr(me, "username", None),
            is_active=True,
        )
        session.add(acct)
        await session.commit()

    otp_buffers.pop(uid, None)
    del pending_clients[uid]
    await state.clear()

    name = getattr(me, "first_name", "?") + (f" {me.last_name}" if getattr(me, "last_name", None) else "")
    text = (
        f"✅ <b>Account Added Successfully!</b>\n\n"
        f"👤 Name: {name}\n"
        f"📱 Phone: {phone}\n"
        f"🆔 TG ID: {me.id}\n\n"
        "Account is ready for broadcasting!"
    )

    if hasattr(event, "message"):
        await event.message.answer(text, reply_markup=kb_back())
    else:
        await event.answer(text, reply_markup=kb_back())


# ─── SET ADVERTISEMENT ────────────────────────────────────────────────────────

@router.callback_query(F.data == "set_ad")
async def cb_set_ad(cb: CallbackQuery, state: FSMContext) -> None:
    async with AsyncSessionLocal() as session:
        cfg = await get_or_create_config(session, cb.from_user.id)
        suffix = cfg.ad_suffix or AD_SUFFIX_DEFAULT

    await cb.message.edit_caption(
        caption=(
            "📢 <b>Ad Message</b>\n\n"
            "<b>Ad message:</b> <i>updated ✅</i>\n\n"
            "<blockquote>💡 Tips for effective ads:\n"
            "• Keep it concise and engaging\n"
            "• Use emojis for flair\n"
            "• Include a clear call-to-action</blockquote>\n\n"
            f"Your ad will end with: <code>{suffix}</code>\n\n"
            "<i>Send your new message to set as your advertisement:</i>"
        ),
        reply_markup=kb_back(),
    )
    await state.set_state(SetAd.waiting_message)
    await cb.answer()


@router.message(SetAd.waiting_message)
async def handle_ad_message(message: Message, state: FSMContext) -> None:
    async with AsyncSessionLocal() as session:
        cfg = await get_or_create_config(session, message.from_user.id)
        cfg.ad_message = message.text or message.caption or ""
        await session.commit()

    await state.clear()
    await message.answer(
        "✅ <b>Advertisement saved!</b>\n\n"
        f"Message: <code>{cfg.ad_message[:100]}...</code>",
        reply_markup=kb_back(),
    )


# ─── INTERVAL ─────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "set_interval")
async def cb_set_interval(cb: CallbackQuery, state: FSMContext) -> None:
    async with AsyncSessionLocal() as session:
        cfg = await get_or_create_config(session, cb.from_user.id)
        cur = cfg.interval_seconds

    await cb.message.edit_caption(
        caption=(
            "⏱ <b>SET BROADCAST CYCLE INTERVAL</b>\n\n"
            f"<b>Current Interval:</b> {cur} seconds\n\n"
            "━━━━━━━━━━━━━━━━\n"
            "<b>Recommended Intervals:</b>\n"
            "• 300s  - Aggressive (5 min) 🔴\n"
            "• 600s  - Safe & Balanced (10 min) 🟡\n"
            "• 1200s - Conservative (20 min) 🟢\n\n"
            "<blockquote>To set custom time interval\n"
            "Send a number (in seconds):\n\n"
            "(Note: using short time interval for broadcasting "
            "can get your Account on high risk.)</blockquote>"
        ),
        reply_markup=kb_interval(),
    )
    await state.set_state(SetInterval.waiting_custom)
    await cb.answer()


@router.callback_query(F.data.startswith("interval:"))
async def cb_interval_preset(cb: CallbackQuery, state: FSMContext) -> None:
    secs = int(cb.data.split(":")[1])
    async with AsyncSessionLocal() as session:
        user = await get_or_create_user(session, cb.from_user)
        min_interval = plan_interval_min(user.plan)
        if secs < min_interval:
            await cb.answer(
                f"❌ Minimum interval for your plan is {min_interval}s. Upgrade to unlock!",
                show_alert=True,
            )
            return
        cfg = await get_or_create_config(session, cb.from_user.id)
        cfg.interval_seconds = secs
        await session.commit()

    await state.clear()
    await cb.message.edit_caption(
        caption=f"✅ Interval set to <b>{secs}s</b> ({secs//60} min)",
        reply_markup=kb_back(),
    )
    await cb.answer()


@router.message(SetInterval.waiting_custom)
async def handle_custom_interval(message: Message, state: FSMContext) -> None:
    if not message.text or not message.text.isdigit():
        await message.answer("❌ Send a number (seconds).")
        return

    secs = int(message.text)
    async with AsyncSessionLocal() as session:
        user = await get_or_create_user(session, message.from_user)
        min_interval = plan_interval_min(user.plan)
        if secs < min_interval:
            await message.answer(
                f"❌ Minimum interval for your plan is {min_interval}s.\n"
                "Upgrade to Premium for shorter intervals!"
            )
            return
        cfg = await get_or_create_config(session, message.from_user.id)
        cfg.interval_seconds = secs
        await session.commit()

    await state.clear()
    await message.answer(
        f"✅ Interval set to <b>{secs}s</b> ({secs//60} min)",
        reply_markup=kb_back(),
    )


# ─── RUN ADS ──────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "run_ads")
async def cb_run_ads(cb: CallbackQuery) -> None:
    uid = cb.from_user.id
    async with AsyncSessionLocal() as session:
        cfg   = await get_or_create_config(session, uid)
        accts = await get_accounts(session, uid)

    if not cfg.ad_message:
        await cb.answer("❌ Set your advertisement first!", show_alert=True)
        return

    active_accts = [a for a in accts if a.is_active and a.session_string]
    if not active_accts:
        await cb.answer("❌ No active accounts. Add an account first!", show_alert=True)
        return

    already_running = uid in broadcast_tasks and not broadcast_tasks[uid].done()
    if already_running:
        # stop
        broadcast_tasks[uid].cancel()
        del broadcast_tasks[uid]
        async with AsyncSessionLocal() as session:
            for a in accts:
                a.broadcast_running = False
            await session.commit()
        await cb.answer("⏹ Broadcasting stopped.", show_alert=True)
    else:
        # start
        task = asyncio.create_task(
            broadcast_loop(uid, cb.bot)
        )
        broadcast_tasks[uid] = task
        await cb.answer("▶️ Broadcasting started!", show_alert=True)

    await cb.message.delete()
    async with AsyncSessionLocal() as session:
        user     = await get_or_create_user(session, cb.from_user)
        bot_name = await get_setting(session, "bot_name", "LazAdsBot 🚀")
        await send_dashboard(cb.message, user, session, bot_name)


async def broadcast_loop(owner_id: int, bot: Bot) -> None:
    """Core broadcast loop for a user's hosted accounts."""
    log.info("Broadcast loop started for user %d", owner_id)
    while True:
        try:
            async with AsyncSessionLocal() as session:
                cfg   = await get_or_create_config(session, owner_id)
                accts = await get_accounts(session, owner_id)

            if not cfg.ad_message:
                break

            full_msg = cfg.ad_message + f"\n\n{cfg.ad_suffix}"

            for acct in accts:
                if not acct.is_active or not acct.session_string:
                    continue
                try:
                    client = TelegramClient(
                        StringSession(acct.session_string), TG_API_ID, TG_API_HASH
                    )
                    await client.connect()

                    # get dialogs (groups only)
                    async for dialog in client.iter_dialogs():
                        try:
                            entity = dialog.entity
                            if hasattr(entity, "megagroup") or hasattr(entity, "broadcast"):
                                await client.send_message(entity, full_msg)
                                await asyncio.sleep(cfg.delay_seconds)
                        except Exception as e:
                            log.debug("Skip dialog %s: %s", dialog.name, e)

                    await client.disconnect()

                    async with AsyncSessionLocal() as session:
                        acct.broadcast_running = True
                        await session.commit()

                    # notify user
                    try:
                        await bot.send_message(
                            owner_id,
                            f"✅ <b>Broadcast cycle complete</b> for <code>{acct.phone}</code>",
                        )
                    except Exception:
                        pass

                except Exception as e:
                    log.error("Broadcast error for account %d: %s", acct.id, e)

            await asyncio.sleep(cfg.interval_seconds)

        except asyncio.CancelledError:
            log.info("Broadcast loop cancelled for user %d", owner_id)
            async with AsyncSessionLocal() as session:
                accts = await get_accounts(session, owner_id)
                for a in accts:
                    a.broadcast_running = False
                await session.commit()
            break
        except Exception as e:
            log.error("Broadcast loop error: %s", e)
            await asyncio.sleep(60)


# ─── AUTO REPLY ───────────────────────────────────────────────────────────────

@router.callback_query(F.data == "auto_reply")
async def cb_auto_reply(cb: CallbackQuery, state: FSMContext) -> None:
    async with AsyncSessionLocal() as session:
        cfg = await get_or_create_config(session, cb.from_user.id)
        active = cfg.auto_reply_on
        msg_set = cfg.auto_reply_msg or "not set"

    await cb.message.edit_caption(
        caption=(
            "💬 <b>Auto reply</b>\n\n"
            f"• Auto reply message: <i>{msg_set[:60]}</i> {'✅' if cfg.auto_reply_msg else '🔴'}\n"
            f"• Auto reply status: <i>{'active ✅' if active else 'inactive 🟥'}</i>\n\n"
            "<blockquote>📌 Note: Auto reply synchronize with Broadcast "
            "so your account will only send automatic replies "
            "during your ads Broadcasting.</blockquote>\n\n"
            "<i>Send your new message to set as your Auto reply:</i>"
        ),
        reply_markup=kb_auto_reply(active),
    )
    await state.set_state(SetAutoReply.waiting_message)
    await cb.answer()


@router.callback_query(F.data == "toggle_auto_reply")
async def cb_toggle_auto_reply(cb: CallbackQuery, state: FSMContext) -> None:
    async with AsyncSessionLocal() as session:
        cfg = await get_or_create_config(session, cb.from_user.id)
        cfg.auto_reply_on = not cfg.auto_reply_on
        await session.commit()
        status = "activated ✅" if cfg.auto_reply_on else "deactivated ⏹"

    await cb.answer(f"Auto reply {status}", show_alert=True)
    await state.clear()
    await cb.message.delete()
    async with AsyncSessionLocal() as session:
        user     = await get_or_create_user(session, cb.from_user)
        bot_name = await get_setting(session, "bot_name", "LazAdsBot 🚀")
        await send_dashboard(cb.message, user, session, bot_name)


@router.message(SetAutoReply.waiting_message)
async def handle_auto_reply_msg(message: Message, state: FSMContext) -> None:
    async with AsyncSessionLocal() as session:
        cfg = await get_or_create_config(session, message.from_user.id)
        cfg.auto_reply_msg = message.text or ""
        await session.commit()

    await state.clear()
    await message.answer("✅ Auto reply message saved!", reply_markup=kb_back())


# ─── AUTO JOIN ────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "auto_join")
async def cb_auto_join(cb: CallbackQuery) -> None:
    async with AsyncSessionLocal() as session:
        cfg = await get_or_create_config(session, cb.from_user.id)
        cfg.auto_join_on = not cfg.auto_join_on
        await session.commit()
        status = "enabled ✅" if cfg.auto_join_on else "disabled ⏹"

    await cb.answer(f"Auto join {status}", show_alert=True)


# ─── PREMIUM ──────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "premium")
async def cb_premium(cb: CallbackQuery) -> None:
    text = (
        "💎 <b>Go Premium</b>\n\n"
        "━━━━ Plans ━━━━\n\n"
        "👑 <b>Pro</b> — ₹99/month\n"
        "• Up to 10 hosted accounts\n"
        "• Min interval: 60s\n"
        "• Priority support\n\n"
        "💎 <b>Business</b> — ₹299/month\n"
        "• Up to 20 hosted accounts\n"
        "• Min interval: 30s\n"
        "• All Pro features\n"
        "• Custom branding\n\n"
        f"Contact {POWERED_BY} to upgrade 👇"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"💬 Contact {POWERED_BY}", url="https://t.me/TechnicalDevs")],
        [InlineKeyboardButton(text="🔙 Back", callback_data="dashboard")],
    ])
    await cb.message.edit_caption(caption=text, reply_markup=kb)
    await cb.answer()


# ═════════════════════════════════════════════════════════════════════════════
#  ADMIN PANEL
# ═════════════════════════════════════════════════════════════════════════════

@router.message(Command("admin"))
async def cmd_admin(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    await message.answer(
        "🛠 <b>Admin Panel</b>\n\nChoose an action:",
        reply_markup=kb_admin_panel(),
    )


@router.callback_query(F.data == "admin_stats")
async def cb_admin_stats(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Unauthorized", show_alert=True)
        return

    async with AsyncSessionLocal() as session:
        total_users_r = await session.execute(select(func.count()).select_from(LazUser))
        total_accts_r = await session.execute(select(func.count()).select_from(HostedAccount))
        total_users   = total_users_r.scalar_one()
        total_accts   = total_accts_r.scalar_one()

    await cb.message.edit_text(
        f"📊 <b>Bot Stats</b>\n\n"
        f"👥 Total users: <b>{total_users}</b>\n"
        f"📱 Hosted accounts: <b>{total_accts}</b>\n"
        f"🔄 Active broadcasts: <b>{len(broadcast_tasks)}</b>",
        reply_markup=kb_back("admin_panel"),
    )
    await cb.answer()


@router.callback_query(F.data == "admin_panel")
async def cb_admin_panel_back(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    await cb.message.edit_text("🛠 <b>Admin Panel</b>", reply_markup=kb_admin_panel())
    await cb.answer()


# ── Edit bot name / suffix ────────────────────────────────────────────────────

@router.callback_query(F.data == "admin_edit_name")
async def cb_admin_edit_name(cb: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cb.from_user.id):
        return
    await cb.message.edit_text(
        "✏️ Send the new bot display name AND default ad suffix.\n\n"
        "Format (two lines):\n"
        "<code>Bot Name Here 🚀\n~ @YourUsername</code>",
        reply_markup=kb_back("admin_panel"),
    )
    await state.set_state(AdminStates.edit_name)
    await cb.answer()


@router.message(AdminStates.edit_name)
async def handle_admin_edit_name(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    lines = message.text.strip().split("\n", 1)
    bot_name = lines[0].strip()
    suffix   = lines[1].strip() if len(lines) > 1 else AD_SUFFIX_DEFAULT

    async with AsyncSessionLocal() as session:
        await set_setting(session, "bot_name", bot_name)
        await set_setting(session, "default_suffix", suffix)

    await state.clear()
    await message.answer(
        f"✅ Updated!\n<b>Bot name:</b> {bot_name}\n<b>Default suffix:</b> {suffix}",
        reply_markup=kb_back("admin_panel"),
    )


# ── Broadcast to all users ────────────────────────────────────────────────────

@router.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(cb: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cb.from_user.id):
        return
    await cb.message.edit_text(
        "📣 Send the message to broadcast to ALL users:",
        reply_markup=kb_back("admin_panel"),
    )
    await state.set_state(AdminStates.broadcast_msg)
    await cb.answer()


@router.message(AdminStates.broadcast_msg)
async def handle_admin_broadcast(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    await state.clear()

    async with AsyncSessionLocal() as session:
        result = await session.execute(select(LazUser.id))
        user_ids = [row[0] for row in result.fetchall()]

    sent = failed = 0
    for uid in user_ids:
        try:
            await message.bot.copy_message(chat_id=uid, from_chat_id=message.chat.id,
                                            message_id=message.message_id)
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)

    await message.answer(
        f"✅ Broadcast done!\n✅ Sent: {sent}\n❌ Failed: {failed}",
        reply_markup=kb_back("admin_panel"),
    )


# ── Grant premium ─────────────────────────────────────────────────────────────

@router.callback_query(F.data == "admin_grant_premium")
async def cb_admin_grant(cb: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cb.from_user.id):
        return
    await cb.message.edit_text(
        "👑 Send user ID and plan:\n<code>123456789 pro</code>\nor\n<code>123456789 business</code>",
        reply_markup=kb_back("admin_panel"),
    )
    await state.set_state(AdminStates.grant_user_id)
    await cb.answer()


@router.message(AdminStates.grant_user_id)
async def handle_admin_grant(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    parts = message.text.strip().split()
    if len(parts) != 2 or not parts[0].isdigit():
        await message.answer("❌ Format: <code>USER_ID plan</code>")
        return

    uid_target = int(parts[0])
    plan       = parts[1].lower()
    if plan not in ("pro", "business", "free"):
        await message.answer("❌ Plan must be: pro, business, or free")
        return

    async with AsyncSessionLocal() as session:
        result = await session.execute(select(LazUser).where(LazUser.id == uid_target))
        target = result.scalar_one_or_none()
        if not target:
            await message.answer("❌ User not found.")
            await state.clear()
            return
        target.plan = plan
        target.plan_expires = datetime.utcnow() + timedelta(days=30)
        await session.commit()

    await state.clear()
    await message.answer(f"✅ User <code>{uid_target}</code> upgraded to <b>{plan}</b>!")

    try:
        await message.bot.send_message(
            uid_target,
            f"🎉 <b>Your plan has been upgraded to {plan.capitalize()}!</b>\n"
            f"Valid for 30 days. Enjoy your premium features!",
        )
    except Exception:
        pass


# ── Ban user ──────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "admin_ban")
async def cb_admin_ban(cb: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cb.from_user.id):
        return
    await cb.message.edit_text(
        "🚫 Send the user ID to ban:",
        reply_markup=kb_back("admin_panel"),
    )
    await state.set_state(AdminStates.ban_user_id)
    await cb.answer()


@router.message(AdminStates.ban_user_id)
async def handle_admin_ban(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    if not message.text or not message.text.isdigit():
        await message.answer("❌ Send a numeric user ID.")
        return

    uid_target = int(message.text.strip())
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(LazUser).where(LazUser.id == uid_target))
        target = result.scalar_one_or_none()
        if not target:
            await message.answer("❌ User not found.")
            await state.clear()
            return
        target.plan = "banned"
        await session.commit()

    await state.clear()
    await message.answer(f"✅ User <code>{uid_target}</code> banned.")


# ═════════════════════════════════════════════════════════════════════════════
#  PREMIUM USER: custom suffix
# ═════════════════════════════════════════════════════════════════════════════

@router.message(Command("setsuffix"))
async def cmd_set_suffix(message: Message) -> None:
    async with AsyncSessionLocal() as session:
        user = await get_or_create_user(session, message.from_user)
        if user.plan not in ("pro", "business") and not is_admin(message.from_user.id):
            await message.answer(
                "❌ Custom suffix is a Premium feature.\n"
                "Upgrade with /premium!"
            )
            return
        args = message.text.split(None, 1)
        if len(args) < 2:
            await message.answer(
                "Usage: <code>/setsuffix ~ @YourHandle</code>"
            )
            return
        new_suffix = args[1].strip()
        cfg = await get_or_create_config(session, message.from_user.id)
        cfg.ad_suffix = new_suffix
        await session.commit()

    await message.answer(f"✅ Your ad suffix is now: <code>{new_suffix}</code>")


# ─── /help ────────────────────────────────────────────────────────────────────

@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    await message.answer(
        "🚀 <b>LazAdsBot Help</b>\n\n"
        "/start — Open dashboard\n"
        "/setsuffix — Set custom ad suffix (Premium)\n"
        "/admin — Admin panel (admins only)\n\n"
        f"Support: {POWERED_BY}"
    )


# ─── ban guard ────────────────────────────────────────────────────────────────

@router.message()
async def guard_banned(message: Message, state: FSMContext) -> None:
    cur = await state.get_state()
    if cur:
        return   # let FSM handle it
    async with AsyncSessionLocal() as session:
        user = await get_or_create_user(session, message.from_user)
        if user.plan == "banned":
            await message.answer("❌ You are banned from using this bot.")
            return
    # ignore unknown messages


# ═════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        stream=sys.stdout,
    )

    await init_db()

    global bot_instance
    bot_instance = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    storage = MemoryStorage()
    dp      = Dispatcher(storage=storage)
    dp.include_router(router)

    # global error handler
    @dp.errors()
    async def error_handler(event) -> bool:
        log.exception("Unhandled: %s", event.exception)
        return True

    log.info("LazAdsBot starting polling...")
    await bot_instance.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot_instance, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())
