# ============================================================
# LazAdsBot — Telegram Userbot Broadcaster
# Powered by @Recoveredz
# ============================================================

from flask import Flask
from threading import Thread

app = Flask(__name__)

@app.route("/")
def home():
    return "LazAdsBot is running.", 200

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
    Message,
    ReplyKeyboardRemove,
)

# ── telethon (userbot) ────────────────────────────────────────────────────────
try:
    from telethon import TelegramClient, errors as tl_errors
    from telethon.sessions import StringSession
    from telethon.tl.functions.channels import JoinChannelRequest
    from telethon.tl.functions.messages import ImportChatInviteRequest
    from telethon.tl.functions.account import UpdateProfileRequest
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

BOT_TOKEN      = os.getenv("BOT_TOKEN", "9214581560:AAGG5GC7KyalFaSs6tt62yZm9uknmzgYITs")
ADMIN_IDS      = [int(x) for x in os.getenv("ADMIN_IDS", "8404630327").split(",") if x.strip()]
BOT_USERNAME   = os.getenv("BOT_USERNAME", "LazAdsbot")
POWERED_BY     = "@Recoveredz"
MUST_JOIN      = "@TechnicalDevs"
SUBSCRIPTION   = "@Recoveredz"
DATABASE_URL   = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://neondb_owner:npg_J2ZtUBWYxz1q"
    "@ep-green-dew-bd44ci17-pooler.c-8.us-east-2.aws.neon.tech/neondb",
)

TG_API_ID      = int(os.getenv("TG_API_ID", "33273205"))
TG_API_HASH    = os.getenv("TG_API_HASH", "7689254a1a42e86687a33754639d7f35")

MAX_FREE_ACCOUNTS  = 2
MAX_PRO_ACCOUNTS   = 10

AD_SUFFIX_DEFAULT  = f"~ @{BOT_USERNAME}"

PLAN_PRICES = {
    "pro":      {"label": "Pro",      "price": "Rs.99/month",  "accounts": 10, "interval_min": 60},
    "business": {"label": "Business", "price": "Rs.299/month", "accounts": 20, "interval_min": 30},
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

    id              : Mapped[int]  = mapped_column(BigInteger, primary_key=True)
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
    auto_join_list  : Mapped[Optional[str]] = mapped_column(Text, nullable=True)   # JSON list of group links
    service_type    : Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    target_groups   : Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_at      : Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())


class BotSettings(Base):
    __tablename__ = "laz_bot_settings"

    key   : Mapped[str] = mapped_column(String(64), primary_key=True)
    value : Mapped[str] = mapped_column(Text, default="")


async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Safe migrations — add columns that may not exist on the live DB
        migrations = [
            "ALTER TABLE laz_broadcast_config ADD COLUMN IF NOT EXISTS auto_join_list TEXT",
            "ALTER TABLE laz_broadcast_config ADD COLUMN IF NOT EXISTS service_type VARCHAR(64)",
            "ALTER TABLE laz_broadcast_config ADD COLUMN IF NOT EXISTS target_groups TEXT",
        ]
        for sql in migrations:
            try:
                await conn.execute(__import__("sqlalchemy").text(sql))
            except Exception as e:
                log.warning("Migration skipped (%s): %s", sql[:60], e)
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
    return 300


def parse_group_links(text: str) -> List[str]:
    """Extract t.me links and usernames from raw text."""
    links = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        # t.me/+xxx (private invite), t.me/username, @username, plain username
        if line.startswith("https://t.me/") or line.startswith("t.me/"):
            links.append(line.replace("t.me/", "https://t.me/"))
        elif line.startswith("@"):
            links.append(line)
        elif re.match(r"^[a-zA-Z][a-zA-Z0-9_]{3,}$", line):
            links.append(f"@{line}")
    return links


# ═════════════════════════════════════════════════════════════════════════════
#  KEYBOARDS  —  professional, no emojis
# ═════════════════════════════════════════════════════════════════════════════

def kb_main_menu(is_admin: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="Add Account", callback_data="add_account")],
        [
            InlineKeyboardButton(text="Set Advertisement", callback_data="set_ad"),
            InlineKeyboardButton(text="Interval & Delay",  callback_data="set_interval"),
        ],
        [InlineKeyboardButton(text="Run Broadcast", callback_data="run_ads")],
        [
            InlineKeyboardButton(text="Auto Reply",  callback_data="auto_reply"),
            InlineKeyboardButton(text="Auto Join",   callback_data="auto_join"),
        ],
        [InlineKeyboardButton(text="Upgrade Plan", callback_data="premium")],
    ]
    if is_admin:
        rows.append([InlineKeyboardButton(text="Admin Panel", callback_data="admin_panel")])
    rows.append([
        InlineKeyboardButton(text="About",      url=f"https://t.me/{BOT_USERNAME}"),
        InlineKeyboardButton(text="Powered by", url=f"https://t.me/{POWERED_BY.lstrip('@')}"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_otp_pad() -> InlineKeyboardMarkup:
    rows = []
    for row in [["1","2","3"],["4","5","6"],["7","8","9"]]:
        rows.append([InlineKeyboardButton(text=d, callback_data=f"otp_digit:{d}") for d in row])
    rows.append([
        InlineKeyboardButton(text="DEL",    callback_data="otp_digit:DEL"),
        InlineKeyboardButton(text="0",      callback_data="otp_digit:0"),
        InlineKeyboardButton(text="Cancel", callback_data="otp_cancel"),
    ])
    rows.append([InlineKeyboardButton(text="Show Code", callback_data="otp_show")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_interval() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="20 min", callback_data="interval:1200"),
            InlineKeyboardButton(text="10 min", callback_data="interval:600"),
            InlineKeyboardButton(text="5 min",  callback_data="interval:300"),
        ],
        [InlineKeyboardButton(text="Back", callback_data="dashboard")],
    ])


def kb_auto_reply(active: bool) -> InlineKeyboardMarkup:
    toggle = "Deactivate Auto Reply" if active else "Activate Auto Reply"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=toggle, callback_data="toggle_auto_reply")],
        [InlineKeyboardButton(text="Back", callback_data="dashboard")],
    ])


def kb_auto_join(active: bool) -> InlineKeyboardMarkup:
    toggle = "Disable Auto Join" if active else "Enable Auto Join"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=toggle,            callback_data="toggle_auto_join")],
        [InlineKeyboardButton(text="Import Group List", callback_data="import_groups")],
        [InlineKeyboardButton(text="Back",            callback_data="dashboard")],
    ])


def kb_back(cb: str = "dashboard") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Back", callback_data=cb)]
    ])


def kb_admin_panel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Edit Bot Name / Suffix",  callback_data="admin_edit_name")],
        [InlineKeyboardButton(text="Broadcast to All Users",  callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="Grant Premium",           callback_data="admin_grant_premium")],
        [InlineKeyboardButton(text="Ban User",                callback_data="admin_ban")],
        [InlineKeyboardButton(text="Statistics",              callback_data="admin_stats")],
        [InlineKeyboardButton(text="Back",                    callback_data="dashboard")],
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


class SetAutoJoin(StatesGroup):
    waiting_groups  = State()   # text or .txt file with group links


class AdminStates(StatesGroup):
    broadcast_msg   = State()
    grant_user_id   = State()
    ban_user_id     = State()
    edit_name       = State()


# ═════════════════════════════════════════════════════════════════════════════
#  GLOBAL STATE
# ═════════════════════════════════════════════════════════════════════════════

active_clients:   Dict[int, Any] = {}
broadcast_tasks:  Dict[int, asyncio.Task] = {}
otp_buffers:      Dict[int, str] = {}
pending_clients:  Dict[int, Any] = {}

bot_instance: Optional[Bot] = None


# ═════════════════════════════════════════════════════════════════════════════
#  FORCE-JOIN CHECK
# ═════════════════════════════════════════════════════════════════════════════

async def check_joined(bot: Bot, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=MUST_JOIN, user_id=user_id)
        return member.status not in ("left", "kicked", "banned")
    except Exception:
        return False


# ═════════════════════════════════════════════════════════════════════════════
#  DASHBOARD  —  text-only, professional layout
# ═════════════════════════════════════════════════════════════════════════════

async def send_dashboard(message: Message, user: LazUser, session: AsyncSession,
                          bot_name: str) -> None:
    cfg   = await get_or_create_config(session, user.id)
    accts = await get_accounts(session, user.id)
    limit = plan_account_limit(user.plan)

    running = any(a.broadcast_running for a in accts)
    status  = "<b>Running</b>" if running else "Stopped"
    plan_label = user.plan.capitalize()
    interval_m = cfg.interval_seconds // 60
    interval_s = cfg.interval_seconds % 60
    interval_label = f"{interval_m}m {interval_s}s" if interval_s else f"{interval_m}m"

    # count imported groups
    group_count = 0
    if cfg.auto_join_list:
        try:
            group_count = len(json.loads(cfg.auto_join_list))
        except Exception:
            pass

    reply_state = "<i>On</i>" if cfg.auto_reply_on else "Off"
    join_state  = f"<i>On</i> <code>({group_count} groups)</code>" if cfg.auto_join_on else "Off"

    text = (
        f"<b>{bot_name}</b>  <i>— Broadcaster</i>\n"
        f"<blockquote>Powered by {POWERED_BY}</blockquote>\n\n"
        f"<b>Accounts</b>       {len(accts)} / {limit}\n"
        f"<b>Broadcast</b>      {status}\n"
        f"<b>Interval</b>       <code>{interval_label}</code>\n"
        f"<b>Auto Reply</b>     {reply_state}\n"
        f"<b>Auto Join</b>      {join_state}\n"
        f"<b>Plan</b>           <i>{plan_label}</i>\n"
    )

    admin = is_admin(user.id)
    await message.answer(text, reply_markup=kb_main_menu(is_admin=admin))


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

    joined = await check_joined(bot, uid)
    if not joined:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"Join {MUST_JOIN}", url=f"https://t.me/{MUST_JOIN.lstrip('@')}")],
            [InlineKeyboardButton(text="I have joined", callback_data="check_join")],
        ])
        await message.answer(
            f"<b>Access Restricted</b>\n"
            f"<blockquote>Membership required to use this bot.</blockquote>\n\n"
            f"Join <b>{MUST_JOIN}</b> and press <i>I have joined</i> below.",
            reply_markup=kb,
        )
        return

    async with AsyncSessionLocal() as session:
        user     = await get_or_create_user(session, message.from_user)
        bot_name = await get_setting(session, "bot_name", "LazAdsBot")
        await send_dashboard(message, user, session, bot_name)


@router.callback_query(F.data == "check_join")
async def cb_check_join(cb: CallbackQuery, state: FSMContext) -> None:
    joined = await check_joined(cb.bot, cb.from_user.id)
    if not joined:
        await cb.answer("You have not joined the channel yet.", show_alert=True)
        return
    await cb.message.delete()
    async with AsyncSessionLocal() as session:
        user     = await get_or_create_user(session, cb.from_user)
        bot_name = await get_setting(session, "bot_name", "LazAdsBot")
        await send_dashboard(cb.message, user, session, bot_name)
    await cb.answer()


@router.callback_query(F.data == "dashboard")
async def cb_dashboard(cb: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    try:
        await cb.message.delete()
    except Exception:
        pass
    async with AsyncSessionLocal() as session:
        user     = await get_or_create_user(session, cb.from_user)
        bot_name = await get_setting(session, "bot_name", "LazAdsBot")
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
            f"Account limit reached ({count}/{limit}). Upgrade your plan.",
            show_alert=True,
        )
        return

    await cb.message.edit_text(
        "<b>Host New Account</b>\n"
        "<blockquote>Your account will be used to send broadcasts.</blockquote>\n\n"
        "Enter your phone number with country code:\n"
        "<code>+91XXXXXXXXXX</code>",
        reply_markup=kb_back(),
    )
    await state.set_state(AddAccount.waiting_phone)
    await cb.answer()


@router.message(AddAccount.waiting_phone)
async def handle_phone(message: Message, state: FSMContext) -> None:
    phone = message.text.strip() if message.text else ""
    if not re.match(r"^\+\d{7,15}$", phone):
        await message.answer("Invalid format. Use: +918824918436")
        return

    if not TELETHON_OK:
        await message.answer("Telethon is not installed. Contact admin.")
        await state.clear()
        return

    wait_msg = await message.answer("<i>Sending OTP…</i>")

    client = TelegramClient(StringSession(), TG_API_ID, TG_API_HASH)
    try:
        await asyncio.wait_for(client.connect(), timeout=15)
    except asyncio.TimeoutError:
        await wait_msg.delete()
        await message.answer("Connection timed out. Try again.")
        await state.clear()
        return

    pending_clients[message.from_user.id] = client
    otp_buffers[message.from_user.id] = ""

    try:
        result = await asyncio.wait_for(
            client.send_code_request(phone), timeout=20
        )
    except asyncio.TimeoutError:
        await wait_msg.delete()
        await message.answer("OTP request timed out. Check the number and try again.")
        await client.disconnect()
        pending_clients.pop(message.from_user.id, None)
        await state.clear()
        return
    except tl_errors.FloodWaitError as e:
        await wait_msg.delete()
        await message.answer(f"Flood wait. Try again in {e.seconds} seconds.")
        await client.disconnect()
        pending_clients.pop(message.from_user.id, None)
        await state.clear()
        return
    except Exception as e:
        await wait_msg.delete()
        await message.answer(f"Error sending OTP: {e}")
        await client.disconnect()
        pending_clients.pop(message.from_user.id, None)
        await state.clear()
        return

    await wait_msg.delete()

    otp_msg = await message.answer(
        f"<b>OTP sent to</b> <code>{phone}</code>\n\n"
        f"Enter the code using the keypad below.\n"
        f"<b>Code:</b>  <code>_____</code>\n\n"
        f"<i>Check your Telegram app for the code.</i>",
        reply_markup=kb_otp_pad(),
        parse_mode="HTML",
    )
    await state.update_data(phone=phone, phone_code_hash=result.phone_code_hash,
                             otp_msg_id=otp_msg.message_id)
    await state.set_state(AddAccount.waiting_otp)


@router.callback_query(F.data.startswith("otp_digit:"))
async def cb_otp_digit(cb: CallbackQuery, state: FSMContext) -> None:
    # Only process if user is in OTP state
    cur_state = await state.get_state()
    if cur_state != AddAccount.waiting_otp:
        await cb.answer()
        return

    digit = cb.data.split(":")[1]
    uid   = cb.from_user.id

    if digit == "DEL":
        otp_buffers[uid] = otp_buffers.get(uid, "")[:-1]
    else:
        buf = otp_buffers.get(uid, "")
        if len(buf) < 5:
            otp_buffers[uid] = buf + digit

    buf     = otp_buffers.get(uid, "")
    display = buf + "_" * (5 - len(buf))
    data    = await state.get_data()
    phone   = data.get("phone", "")

    # Update the OTP message
    try:
        await cb.message.edit_text(
            f"<b>OTP sent to</b> <code>{phone}</code>\n\n"
            f"Enter the code using the keypad below.\n"
            f"<b>Code:</b>  <code>{display}</code>\n\n"
            f"<i>Valid for 5 minutes.</i>",
            reply_markup=kb_otp_pad(),
            parse_mode="HTML",
        )
    except Exception as e:
        if "message is not modified" not in str(e).lower():
            log.warning("OTP edit_text failed: %s", e)

    await cb.answer()

    # auto-submit at 5 digits
    if len(buf) == 5:
        await _submit_otp(cb, state)


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
    try:
        await cb.message.delete()
    except Exception:
        pass
    await cb.answer("Cancelled.")

    async with AsyncSessionLocal() as session:
        user     = await get_or_create_user(session, cb.from_user)
        bot_name = await get_setting(session, "bot_name", "LazAdsBot")
        await send_dashboard(cb.message, user, session, bot_name)


async def _submit_otp(cb: CallbackQuery, state: FSMContext) -> None:
    uid    = cb.from_user.id
    code   = otp_buffers.get(uid, "")
    data   = await state.get_data()
    phone  = data.get("phone", "")
    pch    = data.get("phone_code_hash", "")
    client = pending_clients.get(uid)

    if not client:
        await cb.message.answer("Session expired. Please start again.")
        await state.clear()
        return

    try:
        me = await asyncio.wait_for(
            client.sign_in(phone=phone, code=code, phone_code_hash=pch),
            timeout=20,
        )
    except asyncio.TimeoutError:
        await cb.message.answer("Sign-in timed out. Please try again.")
        await state.clear()
        return
    except tl_errors.SessionPasswordNeededError:
        try:
            await cb.message.edit_text(
                "<b>Two-Step Verification</b>\n\n"
                "Your account has 2FA enabled.\n"
                "Send your password:",
                reply_markup=kb_back("dashboard"),
            )
        except Exception:
            await cb.message.answer(
                "<b>Two-Step Verification</b>\n\nSend your 2FA password:",
                reply_markup=kb_back("dashboard"),
            )
        await state.set_state(AddAccount.waiting_2fa)
        return
    except tl_errors.PhoneCodeInvalidError:
        otp_buffers[uid] = ""
        try:
            await cb.message.edit_text(
                "<b>Incorrect OTP.</b> Please try again.\n\n"
                "<b>Code:</b>  <code>_____</code>",
                reply_markup=kb_otp_pad(),
                parse_mode="HTML",
            )
        except Exception:
            pass
        return
    except tl_errors.PhoneCodeExpiredError:
        otp_buffers[uid] = ""
        await cb.message.answer(
            "OTP expired. Please start the process again.",
            reply_markup=kb_back("dashboard"),
        )
        await state.clear()
        return
    except Exception as e:
        await cb.message.answer(f"Sign-in error: {e}")
        await state.clear()
        return

    await _save_account(cb, state, client, me, phone)


@router.message(AddAccount.waiting_2fa)
async def handle_2fa(message: Message, state: FSMContext) -> None:
    uid    = message.from_user.id
    client = pending_clients.get(uid)
    if not client:
        await message.answer("Session expired. Please start again.")
        await state.clear()
        return

    try:
        me = await client.sign_in(password=message.text.strip() if message.text else "")
    except tl_errors.PasswordHashInvalidError:
        await message.answer("Incorrect password. Try again:")
        return
    except Exception as e:
        await message.answer(f"Error: {e}")
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
    pending_clients.pop(uid, None)
    await state.clear()

    fname = getattr(me, "first_name", "")
    lname = getattr(me, "last_name", "") or ""
    name  = f"{fname} {lname}".strip()

    # Set last name and bio on the added account
    try:
        new_last = f"• Via @{BOT_USERNAME}"
        bio_text = f"Ads via @{BOT_USERNAME}.\nPowered by {MUST_JOIN}"
        await client(UpdateProfileRequest(last_name=new_last, about=bio_text))
    except Exception:
        pass  # best-effort, don't block account add

    await client.disconnect()

    text = (
        f"<b>Account Added</b>\n\n"
        f"<b>Name</b>   {name}\n"
        f"<b>Phone</b>  <code>{phone}</code>\n"
        f"<b>TG ID</b>  <code>{me.id}</code>\n\n"
        f"<i>Account is active and ready to broadcast.</i>"
    )

    if hasattr(event, "message"):
        await event.message.answer(text, reply_markup=kb_back(), parse_mode="HTML")
    else:
        await event.answer(text, reply_markup=kb_back(), parse_mode="HTML")


# ─── SET ADVERTISEMENT ────────────────────────────────────────────────────────

@router.callback_query(F.data == "set_ad")
async def cb_set_ad(cb: CallbackQuery, state: FSMContext) -> None:
    async with AsyncSessionLocal() as session:
        cfg    = await get_or_create_config(session, cb.from_user.id)
        suffix = cfg.ad_suffix or AD_SUFFIX_DEFAULT
        cur_ad = cfg.ad_message or "Not set"

    await cb.message.edit_text(
        "<b>Set Advertisement</b>\n"
        "<blockquote>This message will be sent to all target groups.</blockquote>\n\n"
        f"<b>Current:</b>  <i>{cur_ad[:80]}{'...' if len(cur_ad) > 80 else ''}</i>\n"
        f"<b>Suffix:</b>   <code>{suffix}</code>\n\n"
        "Send your new advertisement message below:",
        reply_markup=kb_back(),
    )
    await state.set_state(SetAd.waiting_message)
    await cb.answer()


@router.message(SetAd.waiting_message)
async def handle_ad_message(message: Message, state: FSMContext) -> None:
    ad_text = message.text or message.caption or ""
    async with AsyncSessionLocal() as session:
        cfg = await get_or_create_config(session, message.from_user.id)
        cfg.ad_message = ad_text
        await session.commit()

    await state.clear()
    await message.answer(
        f"<b>Advertisement saved.</b>\n\n<code>{ad_text[:120]}</code>",
        reply_markup=kb_back(),
    )


# ─── INTERVAL ─────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "set_interval")
async def cb_set_interval(cb: CallbackQuery, state: FSMContext) -> None:
    async with AsyncSessionLocal() as session:
        cfg = await get_or_create_config(session, cb.from_user.id)
        cur = cfg.interval_seconds

    await cb.message.edit_text(
        "<b>Broadcast Interval</b>\n"
        "<blockquote>How often your ad is sent to each group.</blockquote>\n\n"
        f"<b>Current:</b>  <code>{cur}s</code>  <i>({cur // 60} min)</i>\n\n"
        "Choose a preset or send a custom number in seconds.\n"
        "<i>Note: shorter intervals increase account risk.</i>",
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
                f"Minimum interval for your plan is {min_interval}s. Upgrade to unlock shorter intervals.",
                show_alert=True,
            )
            return
        cfg = await get_or_create_config(session, cb.from_user.id)
        cfg.interval_seconds = secs
        await session.commit()

    await state.clear()
    await cb.message.edit_text(
        f"Interval set to <b>{secs}s</b> ({secs // 60} min).",
        reply_markup=kb_back(),
    )
    await cb.answer()


@router.message(SetInterval.waiting_custom)
async def handle_custom_interval(message: Message, state: FSMContext) -> None:
    if not message.text or not message.text.isdigit():
        await message.answer("Send a whole number (seconds).")
        return

    secs = int(message.text)
    async with AsyncSessionLocal() as session:
        user = await get_or_create_user(session, message.from_user)
        min_interval = plan_interval_min(user.plan)
        if secs < min_interval:
            await message.answer(
                f"Minimum interval for your plan is {min_interval}s.\n"
                "Upgrade to access shorter intervals."
            )
            return
        cfg = await get_or_create_config(session, message.from_user.id)
        cfg.interval_seconds = secs
        await session.commit()

    await state.clear()
    await message.answer(
        f"Interval set to <b>{secs}s</b> ({secs // 60} min).",
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
        await cb.answer("Set your advertisement first.", show_alert=True)
        return

    active_accts = [a for a in accts if a.is_active and a.session_string]
    if not active_accts:
        await cb.answer("No active accounts. Add an account first.", show_alert=True)
        return

    already_running = uid in broadcast_tasks and not broadcast_tasks[uid].done()
    if already_running:
        broadcast_tasks[uid].cancel()
        del broadcast_tasks[uid]
        async with AsyncSessionLocal() as session:
            accts2 = await get_accounts(session, uid)
            for a in accts2:
                a.broadcast_running = False
            await session.commit()
        await cb.answer("Broadcast stopped.", show_alert=True)
    else:
        task = asyncio.create_task(broadcast_loop(uid, cb.bot))
        broadcast_tasks[uid] = task
        await cb.answer("Broadcast started.", show_alert=True)

    try:
        await cb.message.delete()
    except Exception:
        pass
    async with AsyncSessionLocal() as session:
        user     = await get_or_create_user(session, cb.from_user)
        bot_name = await get_setting(session, "bot_name", "LazAdsBot")
        await send_dashboard(cb.message, user, session, bot_name)


async def broadcast_loop(owner_id: int, bot: Bot) -> None:
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

                    # auto-join groups if enabled
                    async with AsyncSessionLocal() as s2:
                        cfg2 = await get_or_create_config(s2, owner_id)
                        if cfg2.auto_join_on and cfg2.auto_join_list:
                            try:
                                links = json.loads(cfg2.auto_join_list)
                                for link in links:
                                    try:
                                        if "+t.me/" in link or "joinchat" in link:
                                            invite_hash = link.split("/")[-1].lstrip("+")
                                            await client(ImportChatInviteRequest(invite_hash))
                                        else:
                                            uname = link.lstrip("@").replace("https://t.me/", "")
                                            await client(JoinChannelRequest(uname))
                                        await asyncio.sleep(2)
                                    except Exception as je:
                                        log.debug("Auto-join skip %s: %s", link, je)
                            except Exception:
                                pass

                    # broadcast to all dialogs
                    async for dialog in client.iter_dialogs():
                        try:
                            entity = dialog.entity
                            if hasattr(entity, "megagroup") or hasattr(entity, "broadcast"):
                                await client.send_message(entity, full_msg)
                                await asyncio.sleep(cfg.delay_seconds)
                        except Exception as e:
                            log.debug("Skip dialog %s: %s", dialog.name, e)

                    await client.disconnect()

                    async with AsyncSessionLocal() as s3:
                        result = await s3.execute(
                            select(HostedAccount).where(HostedAccount.id == acct.id)
                        )
                        a = result.scalar_one_or_none()
                        if a:
                            a.broadcast_running = True
                            await s3.commit()

                    try:
                        await bot.send_message(
                            owner_id,
                            f"Broadcast cycle complete for <code>{acct.phone}</code>.",
                        )
                    except Exception:
                        pass

                except Exception as e:
                    log.error("Broadcast error account %d: %s", acct.id, e)

            await asyncio.sleep(cfg.interval_seconds)

        except asyncio.CancelledError:
            log.info("Broadcast loop cancelled for user %d", owner_id)
            async with AsyncSessionLocal() as session:
                accts2 = await get_accounts(session, owner_id)
                for a in accts2:
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
        active  = cfg.auto_reply_on
        cur_msg = cfg.auto_reply_msg or "Not set"

    await cb.message.edit_text(
        "<b>Auto Reply</b>\n"
        "<blockquote>Automatically respond to incoming messages during broadcast.</blockquote>\n\n"
        f"<b>Status:</b>   <i>{'Active' if active else 'Inactive'}</i>\n"
        f"<b>Message:</b>  <i>{cur_msg[:80]}</i>\n\n"
        "Send a new message below to update the auto-reply text.",
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
        status = "Activated" if cfg.auto_reply_on else "Deactivated"

    await cb.answer(f"Auto Reply {status}.", show_alert=True)
    await state.clear()
    try:
        await cb.message.delete()
    except Exception:
        pass
    async with AsyncSessionLocal() as session:
        user     = await get_or_create_user(session, cb.from_user)
        bot_name = await get_setting(session, "bot_name", "LazAdsBot")
        await send_dashboard(cb.message, user, session, bot_name)


@router.message(SetAutoReply.waiting_message)
async def handle_auto_reply_msg(message: Message, state: FSMContext) -> None:
    async with AsyncSessionLocal() as session:
        cfg = await get_or_create_config(session, message.from_user.id)
        cfg.auto_reply_msg = message.text or ""
        await session.commit()

    await state.clear()
    await message.answer("Auto reply message saved.", reply_markup=kb_back())


# ─── AUTO JOIN ────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "auto_join")
async def cb_auto_join(cb: CallbackQuery) -> None:
    async with AsyncSessionLocal() as session:
        cfg   = await get_or_create_config(session, cb.from_user.id)
        active = cfg.auto_join_on
        count  = 0
        if cfg.auto_join_list:
            try:
                count = len(json.loads(cfg.auto_join_list))
            except Exception:
                pass

    await cb.message.edit_text(
        "<b>Auto Join</b>\n"
        "<blockquote>Accounts auto-join your group list before each broadcast cycle.</blockquote>\n\n"
        f"<b>Status:</b>         <i>{'Active' if active else 'Inactive'}</i>\n"
        f"<b>Groups loaded:</b>  <code>{count}</code>\n\n"
        "Press <b>Import Group List</b> to upload a <code>.txt</code> file\n"
        "or paste group links directly (one per line).\n\n"
        "<i>Formats: @username · t.me/link · t.me/+invitelink</i>",
        reply_markup=kb_auto_join(active),
    )
    await cb.answer()


@router.callback_query(F.data == "toggle_auto_join")
async def cb_toggle_auto_join(cb: CallbackQuery) -> None:
    async with AsyncSessionLocal() as session:
        cfg = await get_or_create_config(session, cb.from_user.id)
        cfg.auto_join_on = not cfg.auto_join_on
        await session.commit()
        status = "Enabled" if cfg.auto_join_on else "Disabled"

    await cb.answer(f"Auto Join {status}.", show_alert=True)
    # Refresh the auto join panel
    async with AsyncSessionLocal() as session:
        cfg   = await get_or_create_config(session, cb.from_user.id)
        count = 0
        if cfg.auto_join_list:
            try:
                count = len(json.loads(cfg.auto_join_list))
            except Exception:
                pass

    try:
        await cb.message.edit_text(
            "<b>Auto Join</b>\n"
            "<blockquote>Accounts auto-join your group list before each broadcast cycle.</blockquote>\n\n"
            f"<b>Status:</b>         <i>{'Active' if cfg.auto_join_on else 'Inactive'}</i>\n"
            f"<b>Groups loaded:</b>  <code>{count}</code>\n\n"
            "Press <b>Import Group List</b> to upload a <code>.txt</code> file\n"
            "or paste group links directly (one per line).\n\n"
            "<i>Formats: @username · t.me/link · t.me/+invitelink</i>",
            reply_markup=kb_auto_join(cfg.auto_join_on),
        )
    except Exception:
        pass


@router.callback_query(F.data == "import_groups")
async def cb_import_groups(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.message.edit_text(
        "<b>Import Group List</b>\n\n"
        "Send a <b>.txt file</b> or paste group links as plain text.\n\n"
        "Accepted formats (one per line):\n"
        "<code>@groupusername</code>\n"
        "<code>https://t.me/groupusername</code>\n"
        "<code>https://t.me/+InviteHash</code>\n\n"
        "Existing list will be replaced.",
        reply_markup=kb_back("auto_join_panel"),
    )
    await state.set_state(SetAutoJoin.waiting_groups)
    await cb.answer()


@router.callback_query(F.data == "auto_join_panel")
async def cb_auto_join_panel(cb: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    async with AsyncSessionLocal() as session:
        cfg   = await get_or_create_config(session, cb.from_user.id)
        active = cfg.auto_join_on
        count  = 0
        if cfg.auto_join_list:
            try:
                count = len(json.loads(cfg.auto_join_list))
            except Exception:
                pass
    try:
        await cb.message.edit_text(
            "<b>Auto Join</b>\n\n"
            f"Status:        {'Active' if active else 'Inactive'}\n"
            f"Groups loaded: {count}\n\n"
            "When enabled, your accounts auto-join groups before each cycle.",
            reply_markup=kb_auto_join(active),
        )
    except Exception:
        pass
    await cb.answer()


@router.message(SetAutoJoin.waiting_groups)
async def handle_auto_join_groups(message: Message, state: FSMContext) -> None:
    raw_text = ""

    # Accept .txt document
    if message.document:
        doc = message.document
        if not doc.file_name or not doc.file_name.endswith(".txt"):
            await message.answer("Please send a .txt file or plain text.")
            return
        file_info = await message.bot.get_file(doc.file_id)
        file_bytes = await message.bot.download_file(file_info.file_path)
        raw_text = file_bytes.read().decode("utf-8", errors="ignore")
    elif message.text:
        raw_text = message.text
    else:
        await message.answer("Send a .txt file or paste the group links as text.")
        return

    links = parse_group_links(raw_text)
    if not links:
        await message.answer(
            "No valid group links found.\n"
            "Make sure lines start with @, t.me/, or https://t.me/"
        )
        return

    async with AsyncSessionLocal() as session:
        cfg = await get_or_create_config(session, message.from_user.id)
        cfg.auto_join_list = json.dumps(links)
        await session.commit()

    await state.clear()
    await message.answer(
        f"<b>Group list imported.</b>\n\n"
        f"{len(links)} groups loaded.\n\n"
        f"<i>Sample:</i>\n" + "\n".join(links[:5]) + ("\n..." if len(links) > 5 else ""),
        reply_markup=kb_back("auto_join_panel"),
    )


# ─── PREMIUM ──────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "premium")
async def cb_premium(cb: CallbackQuery) -> None:
    text = (
        "<b>Upgrade Plan</b>\n\n"
        "<blockquote>Unlock more accounts and faster intervals.</blockquote>\n\n"
        "<b>Pro</b>\n"
        "  · Accounts up to <code>10</code>\n"
        "  · Interval from <code>60s</code>\n"
        "  · Priority support\n\n"
        "<b>Business</b>\n"
        "  · Accounts up to <code>20</code>\n"
        "  · Interval from <code>30s</code>\n"
        "  · Custom branding\n"
        "  · All Pro features\n\n"
        f"<i>Message {SUBSCRIPTION} to subscribe.</i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"Contact {SUBSCRIPTION}", url=f"https://t.me/{SUBSCRIPTION.lstrip('@')}")],
        [InlineKeyboardButton(text="Back", callback_data="dashboard")],
    ])
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()


# ═════════════════════════════════════════════════════════════════════════════
#  ADMIN PANEL
# ═════════════════════════════════════════════════════════════════════════════

@router.message(Command("admin"))
async def cmd_admin(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    await message.answer(
        "<b>Admin Panel</b>\n"
        "<blockquote>Manage users, broadcast, and bot settings.</blockquote>",
        reply_markup=kb_admin_panel(),
    )


@router.callback_query(F.data == "admin_stats")
async def cb_admin_stats(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer("Unauthorized.", show_alert=True)
        return

    async with AsyncSessionLocal() as session:
        total_users_r = await session.execute(select(func.count()).select_from(LazUser))
        total_accts_r = await session.execute(select(func.count()).select_from(HostedAccount))
        total_users   = total_users_r.scalar_one()
        total_accts   = total_accts_r.scalar_one()

    await cb.message.edit_text(
        "<b>Statistics</b>\n\n"
        f"Total users:       {total_users}\n"
        f"Hosted accounts:   {total_accts}\n"
        f"Active broadcasts: {len(broadcast_tasks)}",
        reply_markup=kb_back("admin_panel"),
    )
    await cb.answer()


@router.callback_query(F.data == "admin_panel")
async def cb_admin_panel_back(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer("Unauthorized.", show_alert=True)
        return
    await cb.message.edit_text(
        "<b>Admin Panel</b>\n"
        "<blockquote>Manage users, broadcast, and bot settings.</blockquote>",
        reply_markup=kb_admin_panel(),
    )
    await cb.answer()


@router.callback_query(F.data == "admin_edit_name")
async def cb_admin_edit_name(cb: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cb.from_user.id):
        return
    await cb.message.edit_text(
        "<b>Edit Bot Name / Suffix</b>\n\n"
        "Send two lines:\n"
        "Line 1 — bot display name\n"
        "Line 2 — default ad suffix\n\n"
        "<code>LazAdsBot\n~ @LazAdsbot</code>",
        reply_markup=kb_back("admin_panel"),
    )
    await state.set_state(AdminStates.edit_name)
    await cb.answer()


@router.message(AdminStates.edit_name)
async def handle_admin_edit_name(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    lines    = message.text.strip().split("\n", 1) if message.text else [""]
    bot_name = lines[0].strip()
    suffix   = lines[1].strip() if len(lines) > 1 else AD_SUFFIX_DEFAULT

    async with AsyncSessionLocal() as session:
        await set_setting(session, "bot_name", bot_name)
        await set_setting(session, "default_suffix", suffix)

    await state.clear()
    await message.answer(
        f"Updated.\n<b>Name:</b> {bot_name}\n<b>Suffix:</b> {suffix}",
        reply_markup=kb_back("admin_panel"),
    )


@router.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(cb: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cb.from_user.id):
        return
    await cb.message.edit_text(
        "<b>Broadcast to All Users</b>\n\nSend the message:",
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
        result   = await session.execute(select(LazUser.id))
        user_ids = [row[0] for row in result.fetchall()]

    sent = failed = 0
    for uid in user_ids:
        try:
            await message.bot.copy_message(
                chat_id=uid, from_chat_id=message.chat.id,
                message_id=message.message_id
            )
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)

    await message.answer(
        f"Broadcast complete.\nSent: {sent}  |  Failed: {failed}",
        reply_markup=kb_back("admin_panel"),
    )


@router.callback_query(F.data == "admin_grant_premium")
async def cb_admin_grant(cb: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cb.from_user.id):
        return
    await cb.message.edit_text(
        "<b>Grant Premium</b>\n\nSend user ID and plan:\n"
        "<code>123456789 pro</code>  or  <code>123456789 business</code>",
        reply_markup=kb_back("admin_panel"),
    )
    await state.set_state(AdminStates.grant_user_id)
    await cb.answer()


@router.message(AdminStates.grant_user_id)
async def handle_admin_grant(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    parts = message.text.strip().split() if message.text else []
    if len(parts) != 2 or not parts[0].isdigit():
        await message.answer("Format: <code>USER_ID plan</code>")
        return

    uid_target = int(parts[0])
    plan       = parts[1].lower()
    if plan not in ("pro", "business", "free"):
        await message.answer("Plan must be: pro, business, or free")
        return

    async with AsyncSessionLocal() as session:
        result = await session.execute(select(LazUser).where(LazUser.id == uid_target))
        target = result.scalar_one_or_none()
        if not target:
            await message.answer("User not found.")
            await state.clear()
            return
        target.plan = plan
        target.plan_expires = datetime.utcnow() + timedelta(days=30)
        await session.commit()

    await state.clear()
    await message.answer(f"User <code>{uid_target}</code> upgraded to <b>{plan}</b>.")

    try:
        await message.bot.send_message(
            uid_target,
            f"<b>Plan upgraded to {plan.capitalize()}</b>\n\nValid for 30 days.",
        )
    except Exception:
        pass


@router.callback_query(F.data == "admin_ban")
async def cb_admin_ban(cb: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cb.from_user.id):
        return
    await cb.message.edit_text(
        "<b>Ban User</b>\n\nSend the user ID:",
        reply_markup=kb_back("admin_panel"),
    )
    await state.set_state(AdminStates.ban_user_id)
    await cb.answer()


@router.message(AdminStates.ban_user_id)
async def handle_admin_ban(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    if not message.text or not message.text.strip().isdigit():
        await message.answer("Send a numeric user ID.")
        return

    uid_target = int(message.text.strip())
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(LazUser).where(LazUser.id == uid_target))
        target = result.scalar_one_or_none()
        if not target:
            await message.answer("User not found.")
            await state.clear()
            return
        target.plan = "banned"
        await session.commit()

    await state.clear()
    await message.answer(f"User <code>{uid_target}</code> banned.")


# ═════════════════════════════════════════════════════════════════════════════
#  PREMIUM USER: custom suffix
# ═════════════════════════════════════════════════════════════════════════════

@router.message(Command("setsuffix"))
async def cmd_set_suffix(message: Message) -> None:
    async with AsyncSessionLocal() as session:
        user = await get_or_create_user(session, message.from_user)
        if user.plan not in ("pro", "business") and not is_admin(message.from_user.id):
            await message.answer("Custom suffix is a Premium feature. Upgrade your plan.")
            return
        args = message.text.split(None, 1) if message.text else []
        if len(args) < 2:
            await message.answer("Usage: <code>/setsuffix ~ @YourHandle</code>")
            return
        new_suffix = args[1].strip()
        cfg = await get_or_create_config(session, message.from_user.id)
        cfg.ad_suffix = new_suffix
        await session.commit()

    await message.answer(f"Ad suffix set to: <code>{new_suffix}</code>")


# ─── /help ────────────────────────────────────────────────────────────────────

@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    await message.answer(
        "<b>LazAdsBot — Help</b>\n\n"
        "/start       Open dashboard\n"
        "/setsuffix   Set custom ad suffix (Premium)\n"
        "/admin       Admin panel\n\n"
        f"Support: {POWERED_BY}"
    )


# ─── ban guard ────────────────────────────────────────────────────────────────

@router.message()
async def guard_banned(message: Message, state: FSMContext) -> None:
    cur = await state.get_state()
    if cur:
        return
    async with AsyncSessionLocal() as session:
        user = await get_or_create_user(session, message.from_user)
        if user.plan == "banned":
            await message.answer("Your account has been suspended.")


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

    @dp.errors()
    async def error_handler(event) -> bool:
        log.exception("Unhandled error: %s", event.exception)
        return True

    log.info("LazAdsBot starting polling...")
    await bot_instance.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot_instance, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())
