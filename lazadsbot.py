# ============================================================
# LazAdsBot v2 — Full rewrite
# Powered by @Recoveredz | Channel: @TechnicalDevs
# ============================================================
import asyncio
import logging
import os
import random
import re
import string
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from threading import Thread

from flask import Flask

# ── aiogram ──────────────────────────────────────────────────
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

# ── SQLAlchemy ────────────────────────────────────────────────
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Integer,
    String,
    Text,
    select,
    update,
)
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.pool import NullPool

# ── Telethon ──────────────────────────────────────────────────
try:
    from telethon import TelegramClient
    from telethon.errors import (
        FloodWaitError,
        PhoneCodeExpiredError,
        PhoneCodeInvalidError,
        SessionPasswordNeededError,
    )
    from telethon.sessions import StringSession
    from telethon.tl.functions.account import UpdateProfileRequest
    TELETHON_OK = True
except ImportError:
    TELETHON_OK = False

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# ============================================================
# CONFIG
# ============================================================
BOT_TOKEN      = os.getenv("BOT_TOKEN",      "9214581560:AAGG5GC7KyalFaSs6tt62yZm9uknmzgYITs")
ADMIN_IDS      = [int(x) for x in os.getenv("ADMIN_IDS", "8404630327").split(",") if x.strip()]
BOT_USERNAME   = os.getenv("BOT_USERNAME",   "LazAdsbot")
POWERED_BY     = "@Recoveredz"
MUST_JOIN      = "@TechnicalDevs"
SUBSCRIPTION   = "@Recoveredz"
DATABASE_URL   = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://neondb_owner:npg_J2ZtUBWYxz1q@ep-green-dew-bd44ci17-pooler.c-8.us-east-2.aws.neon.tech/neondb"
)
TG_API_ID      = int(os.getenv("TG_API_ID",   "33273205"))
TG_API_HASH    = os.getenv("TG_API_HASH",     "7689254a1a42e86687a33754639d7f35")

PLAN_CONFIG = {
    "free":     {"label": "Free",      "accounts": 2,  "interval_min": 120, "price": "Free"},
    "silver":   {"label": "🥈 Silver",  "accounts": 5,  "interval_min": 90,  "price": "Rs.149/month"},
    "gold":     {"label": "🥇 Gold",    "accounts": 10, "interval_min": 60,  "price": "Rs.249/month"},
    "platinum": {"label": "💎 Platinum","accounts": 20, "interval_min": 30,  "price": "Rs.499/month"},
}

# ============================================================
# CUSTOM EMOJI IDs
# ============================================================
E_SHIELD    = "5251203410396458957"
E_MONITOR   = "5271604874419647061"
E_LINK      = "5282843764451195532"
E_STATUS    = "5411225014148014586"
E_ARROW     = "5416081784641168838"
E_CHECK     = "5416117059207572332"
E_CROSS     = "5206607081334906820"
E_STAR2     = "5210952531676504517"
E_WARN      = "5420323339723881652"
E_EYE       = "5210956306952758910"
E_COMET     = "5456140674028019486"
E_BAN       = "5224607267797606837"
E_LIGHTNING = "5240241223632954241"
E_ADD       = "5397782960512444700"
E_DIAMOND   = "5397916757333654639"
E_GOLD      = "5427168083074628963"
E_CROWN2    = "5438496463044752972"
E_SETTINGS  = "5217822164362739968"
E_KING      = "5341715473882955310"
E_DOWN      = "5406745015365943482"
E_CLOCK     = "5386367538735104399"
E_FREEZE    = "5449449325434266744"
E_SCAN      = "5445267414562389170"
E_CHAT      = "5231012545799666522"
E_TOP       = "5424972470023104089"
E_ALERT     = "5443038326535759644"
E_BROADCAST = "5467538555158943525"
E_USER      = "5415655814079723871"
E_DONE      = "5395695537687123235"


def ce(eid: str, fb: str) -> str:
    return f"<tg-emoji emoji-id='{eid}'>{fb}</tg-emoji>"


# ============================================================
# DB MODELS
# ============================================================
class Base(DeclarativeBase):
    pass


class LazUser(Base):
    __tablename__ = "laz_users"
    id             = Column(BigInteger, primary_key=True)
    username       = Column(String(64),  nullable=True)
    full_name      = Column(String(128), nullable=True)
    plan           = Column(String(16),  default="free")
    plan_expires   = Column(DateTime,    nullable=True)
    joined_channel = Column(Boolean,     default=False)
    created_at     = Column(DateTime,    default=lambda: datetime.now(timezone.utc))
    log_group_id   = Column(BigInteger,  nullable=True)
    notify_on      = Column(Boolean,     default=True)


class HostedAccount(Base):
    __tablename__      = "hosted_accounts"
    id                 = Column(Integer, primary_key=True, autoincrement=True)
    owner_id           = Column(BigInteger, nullable=False)
    phone              = Column(String(32),  nullable=False)
    session_string     = Column(Text,        nullable=True)
    tg_user_id         = Column(BigInteger,  nullable=True)
    tg_username        = Column(String(64),  nullable=True)
    is_active          = Column(Boolean,     default=True)
    broadcast_running  = Column(Boolean,     default=False)
    warmup_mode        = Column(Boolean,     default=False)
    warmup_until       = Column(DateTime,    nullable=True)
    health_ok          = Column(Boolean,     default=True)
    last_health_check  = Column(DateTime,    nullable=True)
    created_at         = Column(DateTime,    default=lambda: datetime.now(timezone.utc))


class BroadcastConfig(Base):
    __tablename__     = "broadcast_configs"
    id                = Column(Integer, primary_key=True, autoincrement=True)
    owner_id          = Column(BigInteger, nullable=False, unique=True)
    ad_message        = Column(Text,       default="")
    ad_media_id       = Column(String(256),nullable=True)
    ad_media_type     = Column(String(16), nullable=True)
    interval_seconds  = Column(Integer,    default=7200)
    delay_seconds     = Column(Integer,    default=5)
    auto_reply_on     = Column(Boolean,    default=False)
    auto_reply_msg    = Column(Text,       default="")
    auto_join_on      = Column(Boolean,    default=False)
    auto_join_list    = Column(Text,       default="")
    target_groups     = Column(Text,       default="")
    service_type      = Column(String(16), default="groups")
    ab_variant_b      = Column(Text,       nullable=True)
    ab_active         = Column(Boolean,    default=False)
    schedule_start    = Column(DateTime,   nullable=True)
    schedule_end      = Column(DateTime,   nullable=True)
    updated_at        = Column(DateTime,   default=lambda: datetime.now(timezone.utc))


class AccessKey(Base):
    __tablename__  = "access_keys"
    id             = Column(Integer, primary_key=True, autoincrement=True)
    key            = Column(String(8),   nullable=False, unique=True)
    plan           = Column(String(16),  nullable=False)
    duration_days  = Column(Integer,     nullable=False)
    used           = Column(Boolean,     default=False)
    used_by        = Column(BigInteger,  nullable=True)
    used_at        = Column(DateTime,    nullable=True)
    created_at     = Column(DateTime,    default=lambda: datetime.now(timezone.utc))
    created_by     = Column(BigInteger,  nullable=True)


class BotSettings(Base):
    __tablename__ = "bot_settings"
    key   = Column(String(64), primary_key=True)
    value = Column(Text, nullable=True)


class AuditLog(Base):
    __tablename__ = "audit_logs"
    id         = Column(Integer, primary_key=True, autoincrement=True)
    admin_id   = Column(BigInteger, nullable=False)
    action     = Column(String(128), nullable=False)
    target     = Column(String(256), nullable=True)
    details    = Column(Text,        nullable=True)
    created_at = Column(DateTime,    default=lambda: datetime.now(timezone.utc))


class AdTemplate(Base):
    __tablename__ = "ad_templates"
    id         = Column(Integer, primary_key=True, autoincrement=True)
    owner_id   = Column(BigInteger, nullable=False)
    name       = Column(String(64), nullable=False)
    message    = Column(Text,       nullable=False)
    media_id   = Column(String(256),nullable=True)
    media_type = Column(String(16), nullable=True)
    created_at = Column(DateTime,   default=lambda: datetime.now(timezone.utc))


class GroupEntry(Base):
    __tablename__ = "group_entries"
    id          = Column(Integer, primary_key=True, autoincrement=True)
    owner_id    = Column(BigInteger, nullable=False)
    account_id  = Column(Integer,    nullable=False)
    group_link  = Column(String(256),nullable=False)
    group_title = Column(String(256),nullable=True)
    is_dead     = Column(Boolean,    default=False)
    category    = Column(String(64), nullable=True)
    last_sent   = Column(DateTime,   nullable=True)
    created_at  = Column(DateTime,   default=lambda: datetime.now(timezone.utc))


# ============================================================
# DB ENGINE
# ============================================================
engine = create_async_engine(
    DATABASE_URL,
    poolclass=NullPool,
    connect_args={"ssl": "require"},
)
AsyncSessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


@asynccontextmanager
async def get_db():
    async with AsyncSessionLocal() as s:
        yield s


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        import sqlalchemy
        migrations = [
            "ALTER TABLE laz_users ADD COLUMN IF NOT EXISTS log_group_id BIGINT",
            "ALTER TABLE laz_users ADD COLUMN IF NOT EXISTS notify_on BOOLEAN DEFAULT TRUE",
            "ALTER TABLE hosted_accounts ADD COLUMN IF NOT EXISTS warmup_mode BOOLEAN DEFAULT FALSE",
            "ALTER TABLE hosted_accounts ADD COLUMN IF NOT EXISTS warmup_until TIMESTAMP",
            "ALTER TABLE hosted_accounts ADD COLUMN IF NOT EXISTS health_ok BOOLEAN DEFAULT TRUE",
            "ALTER TABLE hosted_accounts ADD COLUMN IF NOT EXISTS last_health_check TIMESTAMP",
            "ALTER TABLE broadcast_configs ADD COLUMN IF NOT EXISTS ad_media_id VARCHAR(256)",
            "ALTER TABLE broadcast_configs ADD COLUMN IF NOT EXISTS ad_media_type VARCHAR(16)",
            "ALTER TABLE broadcast_configs ADD COLUMN IF NOT EXISTS ab_variant_b TEXT",
            "ALTER TABLE broadcast_configs ADD COLUMN IF NOT EXISTS ab_active BOOLEAN DEFAULT FALSE",
            "ALTER TABLE broadcast_configs ADD COLUMN IF NOT EXISTS schedule_start TIMESTAMP",
            "ALTER TABLE broadcast_configs ADD COLUMN IF NOT EXISTS schedule_end TIMESTAMP",
        ]
        for sql in migrations:
            try:
                await conn.execute(sqlalchemy.text(sql))
            except Exception:
                pass
    log.info("DB initialised")


# ============================================================
# HELPERS
# ============================================================
async def get_or_create_user(session: AsyncSession, user_id: int,
                              username: str = None, full_name: str = None) -> LazUser:
    r = await session.execute(select(LazUser).where(LazUser.id == user_id))
    u = r.scalar_one_or_none()
    if not u:
        u = LazUser(id=user_id, username=username, full_name=full_name, plan="free")
        session.add(u)
        await session.commit()
    return u


async def get_or_create_config(session: AsyncSession, owner_id: int) -> BroadcastConfig:
    r = await session.execute(
        select(BroadcastConfig).where(BroadcastConfig.owner_id == owner_id)
    )
    cfg = r.scalar_one_or_none()
    if not cfg:
        cfg = BroadcastConfig(owner_id=owner_id)
        session.add(cfg)
        await session.commit()
    return cfg


async def get_setting(session: AsyncSession, key: str, default: str = "") -> str:
    r = await session.execute(select(BotSettings).where(BotSettings.key == key))
    s = r.scalar_one_or_none()
    return s.value if s else default


async def set_setting(session: AsyncSession, key: str, value: str):
    r = await session.execute(select(BotSettings).where(BotSettings.key == key))
    s = r.scalar_one_or_none()
    if s:
        s.value = value
    else:
        session.add(BotSettings(key=key, value=value))
    await session.commit()


async def count_accounts(session: AsyncSession, owner_id: int) -> int:
    r = await session.execute(
        select(HostedAccount).where(
            HostedAccount.owner_id == owner_id,
            HostedAccount.is_active == True,
        )
    )
    return len(r.scalars().all())


async def get_accounts(session: AsyncSession, owner_id: int):
    r = await session.execute(
        select(HostedAccount).where(
            HostedAccount.owner_id == owner_id,
            HostedAccount.is_active == True,
        )
    )
    return r.scalars().all()


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def plan_account_limit(plan: str) -> int:
    return PLAN_CONFIG.get(plan, PLAN_CONFIG["free"])["accounts"]


def plan_interval_min(plan: str) -> int:
    return PLAN_CONFIG.get(plan, PLAN_CONFIG["free"])["interval_min"]


def parse_group_links(text: str) -> list:
    links = re.findall(r"(?:https?://)?t\.me/[a-zA-Z0-9_]+", text)
    handles = re.findall(r"@[a-zA-Z0-9_]+", text)
    return list(set(links + handles))


def gen_key(length: int = 6) -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))


def plan_is_active(user: LazUser) -> bool:
    if user.plan == "free":
        return True
    if not user.plan_expires:
        return False
    return datetime.now(timezone.utc) <= user.plan_expires.replace(tzinfo=timezone.utc)


async def add_audit(session: AsyncSession, admin_id: int, action: str,
                    target: str = None, details: str = None):
    session.add(AuditLog(admin_id=admin_id, action=action, target=target, details=details))
    await session.commit()


# ============================================================
# FLASK KEEP-ALIVE
# ============================================================
flask_app = Flask(__name__)


@flask_app.route("/")
def home():
    return "LazAdsBot v2 is running 🚀"


@flask_app.route("/health")
def health():
    return "OK", 200


def run_flask():
    flask_app.run(host="0.0.0.0", port=10000)


# ============================================================
# FSM STATES
# ============================================================
class AddAccount(StatesGroup):
    waiting_phone = State()
    waiting_otp   = State()
    waiting_2fa   = State()


class BroadcastSetup(StatesGroup):
    waiting_message = State()
    waiting_media   = State()


class AdminAction(StatesGroup):
    waiting_key_plan     = State()
    waiting_key_duration = State()
    waiting_key_count    = State()
    waiting_announce     = State()
    waiting_key_revoke   = State()


class UserConfig(StatesGroup):
    waiting_interval      = State()
    waiting_delay         = State()
    waiting_reply_msg     = State()
    waiting_groups        = State()
    waiting_log_group     = State()
    waiting_template_name = State()
    waiting_template_msg  = State()


# ============================================================
# KEYBOARDS
# ============================================================
def ik(*rows) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=list(rows))


def btn(text: str, cb: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text=text, callback_data=cb)


def url_btn(text: str, url: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text=text, url=url)


def kb_main_menu(plan: str = "free", is_admin_user: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [btn("📢 Broadcast", "bc_menu"), btn("👤 My Accounts", "accounts_menu")],
        [btn("⚙️ Configure", "config_menu"), btn("📊 Status", "my_status")],
        [btn("💎 Upgrade Plan", "upgrade_menu"), btn("❓ Help", "help_menu")],
    ]
    if is_admin_user:
        rows.append([btn("🛡 Admin Panel", "admin_panel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_accounts_menu() -> InlineKeyboardMarkup:
    return ik(
        [btn("➕ Add Account", "add_account")],
        [btn("📋 List Accounts", "list_accounts")],
        [btn("🔍 Health Check", "health_check")],
        [btn("❄️ Warmup Mode", "warmup_menu")],
        [btn("🗑 Remove Account", "remove_account_menu")],
        [btn("◀️ Back", "main_menu")],
    )


def kb_broadcast_menu() -> InlineKeyboardMarkup:
    return ik(
        [btn("✏️ Set Ad Message", "set_ad_msg")],
        [btn("🖼 Set Ad Media", "set_ad_media")],
        [btn("▶️ Start Broadcast", "start_broadcast")],
        [btn("⏹ Stop Broadcast", "stop_broadcast")],
        [btn("🗂 Templates", "templates_menu")],
        [btn("🔀 A/B Test", "ab_menu")],
        [btn("⏰ Scheduler", "scheduler_menu")],
        [btn("◀️ Back", "main_menu")],
    )


def kb_config_menu() -> InlineKeyboardMarkup:
    return ik(
        [btn("⏱ Set Interval", "cfg_interval"), btn("⏳ Set Delay", "cfg_delay")],
        [btn("💬 Auto Reply", "cfg_reply"),      btn("🔗 Auto Join", "cfg_join")],
        [btn("👥 Target Groups", "cfg_groups"),  btn("📋 Group Categories", "cfg_categories")],
        [btn("📌 Log Group", "cfg_log_group"),   btn("🔔 Notifications", "cfg_notify")],
        [btn("🧹 Clean Dead Groups", "cfg_clean_dead")],
        [btn("◀️ Back", "main_menu")],
    )


def kb_upgrade_menu() -> InlineKeyboardMarkup:
    return ik(
        [btn("🥈 Silver — Rs.149/month", "upgrade_silver")],
        [btn("🥇 Gold — Rs.249/month",   "upgrade_gold")],
        [btn("💎 Platinum — Rs.499/month", "upgrade_platinum")],
        [url_btn("💳 Pay & Get Key", f"https://t.me/{SUBSCRIPTION.lstrip('@')}")],
        [btn("🔑 Enter Access Key", "enter_key")],
        [btn("◀️ Back", "main_menu")],
    )


def kb_admin_panel() -> InlineKeyboardMarkup:
    return ik(
        [btn("🔑 Generate Key",  "admin_gen_key"),   btn("📋 List Keys",  "admin_list_keys")],
        [btn("❌ Revoke Key",    "admin_revoke_key"), btn("👥 Users",      "admin_users")],
        [btn("⏹ Force Stop All","admin_force_stop"), btn("📢 Announce",   "admin_announce")],
        [btn("⚙️ Bot Config",   "admin_bot_config"),  btn("📊 Stats",     "admin_stats")],
        [btn("📜 Audit Log",    "admin_audit"),        btn("💾 Backup",   "admin_backup")],
        [btn("◀️ Back", "main_menu")],
    )


def kb_back(cb: str = "main_menu") -> InlineKeyboardMarkup:
    return ik([btn("◀️ Back", cb)])


# ============================================================
# ROUTERS / GLOBALS
# ============================================================
router = Router()
bot_instance: Bot = None
_broadcast_tasks: dict = {}


# ============================================================
# UTILS
# ============================================================
async def edit_or_send(event, text: str, markup: InlineKeyboardMarkup = None):
    kwargs = dict(text=text, parse_mode="HTML", reply_markup=markup)
    if isinstance(event, CallbackQuery):
        try:
            await event.message.edit_text(**kwargs)
            return
        except Exception:
            pass
        await event.message.answer(**kwargs)
    else:
        await event.answer(**kwargs)


# ============================================================
# /start
# ============================================================
@router.message(CommandStart())
async def cmd_start(msg: Message):
    async with get_db() as s:
        u = await get_or_create_user(s, msg.from_user.id,
                                     msg.from_user.username,
                                     msg.from_user.full_name)
    admin = is_admin(msg.from_user.id)
    plan_label = PLAN_CONFIG.get(u.plan, PLAN_CONFIG["free"])["label"]
    text = (
        f"{ce(E_BROADCAST,'📢')} <b>Welcome to LazAdsBot!</b>\n\n"
        f"{ce(E_USER,'👤')} Plan: <b>{plan_label}</b>\n"
        f"{ce(E_GOLD,'⭐')} Max Accounts: <b>{plan_account_limit(u.plan)}</b>\n\n"
        f"Use the menu below to get started.\n"
        f"Support: {POWERED_BY} | Channel: {MUST_JOIN}"
    )
    await msg.answer(text, parse_mode="HTML",
                     reply_markup=kb_main_menu(u.plan, admin))


# ── /verify ───────────────────────────────────────────────────
@router.message(Command("verify"))
async def cmd_verify(msg: Message):
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        await msg.answer("Usage: /verify YOURKEY")
        return
    key_str = parts[1].strip().upper()
    async with get_db() as s:
        r = await s.execute(select(AccessKey).where(AccessKey.key == key_str))
        ak = r.scalar_one_or_none()
        if not ak:
            await msg.answer(f"{ce(E_CROSS,'❌')} <b>Invalid key.</b>", parse_mode="HTML")
            return
        if ak.used:
            await msg.answer(f"{ce(E_WARN,'⚠️')} <b>Key already used.</b>", parse_mode="HTML")
            return
        u = await get_or_create_user(s, msg.from_user.id)
        u.plan = ak.plan
        u.plan_expires = datetime.now(timezone.utc) + timedelta(days=ak.duration_days)
        ak.used = True
        ak.used_by = msg.from_user.id
        ak.used_at = datetime.now(timezone.utc)
        await s.commit()
        plan_label = PLAN_CONFIG.get(ak.plan, {}).get("label", ak.plan)
    await msg.answer(
        f"{ce(E_DONE,'✅')} <b>Plan activated!</b>\n\n"
        f"Plan: <b>{plan_label}</b>\n"
        f"Expires: <b>{u.plan_expires.strftime('%Y-%m-%d')}</b>",
        parse_mode="HTML"
    )


# ── /status ───────────────────────────────────────────────────
@router.message(Command("status"))
async def cmd_status(msg: Message):
    async with get_db() as s:
        u = await get_or_create_user(s, msg.from_user.id)
        accounts = await get_accounts(s, msg.from_user.id)
        cfg = await get_or_create_config(s, msg.from_user.id)
    plan_label = PLAN_CONFIG.get(u.plan, PLAN_CONFIG["free"])["label"]
    running = sum(1 for a in accounts if a.broadcast_running)
    exp = u.plan_expires.strftime("%Y-%m-%d") if u.plan_expires else "—"
    text = (
        f"{ce(E_STATUS,'📊')} <b>Your Status</b>\n\n"
        f"Plan: <b>{plan_label}</b>\n"
        f"Expires: <b>{exp}</b>\n"
        f"Accounts: <b>{len(accounts)}</b> / {plan_account_limit(u.plan)}\n"
        f"Broadcasting: <b>{running}</b> running\n"
        f"Interval: <b>{cfg.interval_seconds // 60}m</b>\n"
        f"Delay: <b>{cfg.delay_seconds}s</b>"
    )
    await msg.answer(text, parse_mode="HTML", reply_markup=kb_back("main_menu"))


# ── /validity ─────────────────────────────────────────────────
@router.message(Command("validity"))
async def cmd_validity(msg: Message):
    async with get_db() as s:
        u = await get_or_create_user(s, msg.from_user.id)
    if u.plan == "free" or not u.plan_expires:
        await msg.answer("You are on the <b>Free</b> plan (no expiry).", parse_mode="HTML")
        return
    exp = u.plan_expires.replace(tzinfo=timezone.utc)
    days = (exp - datetime.now(timezone.utc)).days
    plan_label = PLAN_CONFIG.get(u.plan, {}).get("label", u.plan)
    await msg.answer(
        f"{ce(E_CLOCK,'⌛')} <b>Plan Validity</b>\n\n"
        f"Plan: <b>{plan_label}</b>\n"
        f"Expires: <b>{exp.strftime('%Y-%m-%d')}</b>\n"
        f"Remaining: <b>{days} day(s)</b>",
        parse_mode="HTML"
    )


# ── /help ─────────────────────────────────────────────────────
@router.message(Command("help"))
async def cmd_help(msg: Message):
    text = (
        f"{ce(E_GOLD,'⭐')} <b>LazAdsBot Commands</b>\n\n"
        "/start — Main menu\n"
        "/verify KEY — Activate an access key\n"
        "/status — Plan & account status\n"
        "/validity — Check plan expiry\n"
        "/stop — Stop all broadcasts\n"
        "/change — Change ad message\n"
        "/help — This message\n\n"
        f"Support: {POWERED_BY}\n"
        f"Channel: {MUST_JOIN}"
    )
    await msg.answer(text, parse_mode="HTML")


# ── /stop ─────────────────────────────────────────────────────
@router.message(Command("stop"))
async def cmd_stop(msg: Message):
    owner_id = msg.from_user.id
    if owner_id in _broadcast_tasks:
        _broadcast_tasks[owner_id].cancel()
        del _broadcast_tasks[owner_id]
    async with get_db() as s:
        await s.execute(
            update(HostedAccount)
            .where(HostedAccount.owner_id == owner_id)
            .values(broadcast_running=False)
        )
        await s.commit()
    await msg.answer(f"{ce(E_CROSS,'❌')} <b>All broadcasts stopped.</b>", parse_mode="HTML")


# ── /change ───────────────────────────────────────────────────
@router.message(Command("change"))
async def cmd_change(msg: Message, state: FSMContext):
    await state.set_state(BroadcastSetup.waiting_message)
    await msg.answer(
        f"{ce(E_CHAT,'💬')} <b>Send your new ad message:</b>\n"
        "HTML formatting supported. /cancel to abort.",
        parse_mode="HTML"
    )


# ── /cancel ───────────────────────────────────────────────────
@router.message(Command("cancel"))
async def cmd_cancel(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer(f"{ce(E_CHECK,'✔️')} Cancelled.", parse_mode="HTML")


# ============================================================
# CALLBACKS — NAVIGATION
# ============================================================
@router.callback_query(F.data == "main_menu")
async def cb_main_menu(cb: CallbackQuery):
    async with get_db() as s:
        u = await get_or_create_user(s, cb.from_user.id)
    plan_label = PLAN_CONFIG.get(u.plan, PLAN_CONFIG["free"])["label"]
    await edit_or_send(cb,
        f"{ce(E_BROADCAST,'📢')} <b>LazAdsBot</b>\nPlan: <b>{plan_label}</b>",
        kb_main_menu(u.plan, is_admin(cb.from_user.id)))
    await cb.answer()


@router.callback_query(F.data == "accounts_menu")
async def cb_accounts_menu(cb: CallbackQuery):
    async with get_db() as s:
        u = await get_or_create_user(s, cb.from_user.id)
        accounts = await get_accounts(s, cb.from_user.id)
    limit = plan_account_limit(u.plan)
    await edit_or_send(cb,
        f"{ce(E_USER,'👤')} <b>Accounts: {len(accounts)}/{limit}</b>",
        kb_accounts_menu())
    await cb.answer()


@router.callback_query(F.data == "bc_menu")
async def cb_bc_menu(cb: CallbackQuery):
    await edit_or_send(cb,
        f"{ce(E_BROADCAST,'📢')} <b>Broadcast Menu</b>",
        kb_broadcast_menu())
    await cb.answer()


@router.callback_query(F.data == "config_menu")
async def cb_config_menu(cb: CallbackQuery):
    await edit_or_send(cb,
        f"{ce(E_SETTINGS,'⚙️')} <b>Configure Bot</b>",
        kb_config_menu())
    await cb.answer()


@router.callback_query(F.data == "upgrade_menu")
async def cb_upgrade_menu(cb: CallbackQuery):
    text = (
        f"{ce(E_DIAMOND,'💎')} <b>Upgrade Your Plan</b>\n\n"
        f"<b>🥈 Silver</b> — Rs.149/month\n  • 5 accounts • 90min interval\n\n"
        f"<b>🥇 Gold</b> — Rs.249/month\n  • 10 accounts • 60min interval\n\n"
        f"<b>💎 Platinum</b> — Rs.499/month\n  • 20 accounts • 30min interval\n\n"
        f"Pay via {SUBSCRIPTION} then enter your key."
    )
    await edit_or_send(cb, text, kb_upgrade_menu())
    await cb.answer()


@router.callback_query(F.data.in_({"upgrade_silver", "upgrade_gold", "upgrade_platinum"}))
async def cb_upgrade_plan(cb: CallbackQuery):
    plan = cb.data.split("_", 1)[1]
    p = PLAN_CONFIG.get(plan, {})
    await cb.answer(
        f"Pay via {SUBSCRIPTION} then use /verify KEY to activate {p.get('label', plan)}.",
        show_alert=True
    )


@router.callback_query(F.data == "my_status")
async def cb_my_status(cb: CallbackQuery):
    async with get_db() as s:
        u = await get_or_create_user(s, cb.from_user.id)
        accounts = await get_accounts(s, cb.from_user.id)
        cfg = await get_or_create_config(s, cb.from_user.id)
    plan_label = PLAN_CONFIG.get(u.plan, PLAN_CONFIG["free"])["label"]
    running = sum(1 for a in accounts if a.broadcast_running)
    exp = u.plan_expires.strftime("%Y-%m-%d") if u.plan_expires else "—"
    text = (
        f"{ce(E_STATUS,'📊')} <b>Status</b>\n\n"
        f"Plan: <b>{plan_label}</b>  |  Expires: <b>{exp}</b>\n"
        f"Accounts: <b>{len(accounts)}</b>/{plan_account_limit(u.plan)}  |  "
        f"Running: <b>{running}</b>\n"
        f"Interval: <b>{cfg.interval_seconds//60}m</b>  |  "
        f"Delay: <b>{cfg.delay_seconds}s</b>"
    )
    await edit_or_send(cb, text, kb_back("main_menu"))
    await cb.answer()


@router.callback_query(F.data == "help_menu")
async def cb_help_menu(cb: CallbackQuery):
    text = (
        f"{ce(E_GOLD,'⭐')} <b>Help</b>\n\n"
        "/start /verify /status /validity /stop /change /help\n\n"
        f"Support: {POWERED_BY}"
    )
    await edit_or_send(cb, text, kb_back("main_menu"))
    await cb.answer()


@router.callback_query(F.data == "enter_key")
async def cb_enter_key(cb: CallbackQuery):
    await cb.message.answer("Send: /verify YOURKEY")
    await cb.answer()


# ============================================================
# ADD ACCOUNT
# ============================================================
@router.callback_query(F.data == "add_account")
async def cb_add_account(cb: CallbackQuery, state: FSMContext):
    if not TELETHON_OK:
        await cb.answer("Telethon not available.", show_alert=True)
        return
    async with get_db() as s:
        u = await get_or_create_user(s, cb.from_user.id)
        count = await count_accounts(s, cb.from_user.id)
    if count >= plan_account_limit(u.plan):
        await cb.answer(
            f"Account limit reached ({count}/{plan_account_limit(u.plan)}). Upgrade your plan.",
            show_alert=True
        )
        return
    await state.set_state(AddAccount.waiting_phone)
    await cb.message.answer(
        f"{ce(E_ADD,'➕')} <b>Add Account</b>\n\n"
        "Send your phone number in international format:\n"
        "<code>+91XXXXXXXXXX</code>\n\n"
        f"{ce(E_WARN,'⚠️')} 2FA supported — you'll be asked if needed.",
        parse_mode="HTML"
    )
    await cb.answer()


@router.message(AddAccount.waiting_phone)
async def handle_phone(msg: Message, state: FSMContext):
    phone = msg.text.strip()
    if not (phone.startswith("+") and phone[1:].isdigit() and len(phone) > 7):
        await msg.answer(
            f"{ce(E_CROSS,'❌')} Invalid format. Use: <code>+91XXXXXXXXXX</code>",
            parse_mode="HTML"
        )
        return
    wait_msg = await msg.answer(f"{ce(E_CLOCK,'⌛')} Requesting OTP…", parse_mode="HTML")
    client = TelegramClient(StringSession(), TG_API_ID, TG_API_HASH)
    try:
        await asyncio.wait_for(client.connect(), timeout=15)
        result = await asyncio.wait_for(client.send_code_request(phone), timeout=20)
    except FloodWaitError as e:
        await wait_msg.edit_text(
            f"{ce(E_WARN,'⚠️')} Flood wait — please wait <b>{e.seconds}s</b> then try again.",
            parse_mode="HTML"
        )
        await client.disconnect()
        await state.clear()
        return
    except asyncio.TimeoutError:
        await wait_msg.edit_text(f"{ce(E_WARN,'⚠️')} Timeout. Try again.", parse_mode="HTML")
        await client.disconnect()
        await state.clear()
        return
    except Exception as e:
        await wait_msg.edit_text(f"{ce(E_CROSS,'❌')} Error: <code>{e}</code>", parse_mode="HTML")
        await client.disconnect()
        await state.clear()
        return

    session_str = client.session.save()
    await client.disconnect()
    await state.update_data(
        phone=phone,
        phone_code_hash=result.phone_code_hash,
        session_str=session_str,
    )
    await state.set_state(AddAccount.waiting_otp)
    await wait_msg.edit_text(
        f"{ce(E_CHAT,'💬')} <b>OTP sent to your Telegram app!</b>\n\n"
        "Enter the 5-digit code:",
        parse_mode="HTML"
    )


@router.message(AddAccount.waiting_otp)
async def handle_otp(msg: Message, state: FSMContext):
    otp = msg.text.strip().replace(" ", "").replace("-", "")
    data = await state.get_data()
    wait_msg = await msg.answer(f"{ce(E_CLOCK,'⌛')} Verifying…", parse_mode="HTML")
    client = TelegramClient(StringSession(data["session_str"]), TG_API_ID, TG_API_HASH)
    try:
        await asyncio.wait_for(client.connect(), timeout=15)
        me = await asyncio.wait_for(
            client.sign_in(data["phone"], otp, phone_code_hash=data["phone_code_hash"]),
            timeout=20
        )
    except SessionPasswordNeededError:
        sess2 = client.session.save()
        await client.disconnect()
        await state.update_data(session_str=sess2)
        await state.set_state(AddAccount.waiting_2fa)
        await wait_msg.edit_text(
            f"{ce(E_WARN,'⚠️')} <b>2FA enabled!</b>\n\nEnter your Telegram password:",
            parse_mode="HTML"
        )
        return
    except PhoneCodeInvalidError:
        await wait_msg.edit_text(f"{ce(E_CROSS,'❌')} Wrong OTP. Try again or restart.")
        await client.disconnect()
        await state.clear()
        return
    except PhoneCodeExpiredError:
        await wait_msg.edit_text(f"{ce(E_CROSS,'❌')} OTP expired. Add account again.")
        await client.disconnect()
        await state.clear()
        return
    except asyncio.TimeoutError:
        await wait_msg.edit_text(f"{ce(E_WARN,'⚠️')} Timeout. Try again.")
        await client.disconnect()
        await state.clear()
        return
    except Exception as e:
        await wait_msg.edit_text(f"{ce(E_CROSS,'❌')} Error: <code>{e}</code>", parse_mode="HTML")
        await client.disconnect()
        await state.clear()
        return

    await _save_account(wait_msg, state, client, me, data["phone"])


@router.message(AddAccount.waiting_2fa)
async def handle_2fa(msg: Message, state: FSMContext):
    data = await state.get_data()
    wait_msg = await msg.answer(f"{ce(E_CLOCK,'⌛')} Signing in…", parse_mode="HTML")
    client = TelegramClient(StringSession(data["session_str"]), TG_API_ID, TG_API_HASH)
    try:
        await asyncio.wait_for(client.connect(), timeout=15)
        me = await asyncio.wait_for(client.sign_in(password=msg.text.strip()), timeout=20)
    except Exception as e:
        await wait_msg.edit_text(f"{ce(E_CROSS,'❌')} 2FA failed: <code>{e}</code>",
                                  parse_mode="HTML")
        await client.disconnect()
        await state.clear()
        return
    await _save_account(wait_msg, state, client, me, data["phone"])


async def _save_account(event: Message, state: FSMContext, client, me, phone: str):
    new_session = client.session.save()
    owner_id = event.chat.id

    async with get_db() as s:
        acct = HostedAccount(
            owner_id=owner_id,
            phone=phone,
            session_string=new_session,
            tg_user_id=me.id,
            tg_username=me.username,
        )
        s.add(acct)
        await s.commit()

    # Apply branding (name + bio)
    try:
        if not client.is_connected():
            await asyncio.wait_for(client.connect(), timeout=15)
        await asyncio.wait_for(
            client(UpdateProfileRequest(
                last_name=f"• Via @{BOT_USERNAME}",
                about=f"Ads via @{BOT_USERNAME}.\nPowered by {MUST_JOIN}",
            )),
            timeout=15,
        )
        log.info("Branding applied to account id=%s", me.id)
    except Exception as e:
        log.warning("Branding failed for account id=%s: %s", me.id, e)
    finally:
        await client.disconnect()

    await state.clear()
    display = me.username or me.first_name or "Unknown"
    await event.edit_text(
        f"{ce(E_DONE,'✅')} <b>Account added!</b>\n\n"
        f"<b>@{display}</b>\n"
        f"Name + bio updated with LazAdsBot branding.",
        parse_mode="HTML",
        reply_markup=kb_back("accounts_menu")
    )


# ============================================================
# LIST / HEALTH / WARMUP / REMOVE ACCOUNTS
# ============================================================
@router.callback_query(F.data == "list_accounts")
async def cb_list_accounts(cb: CallbackQuery):
    async with get_db() as s:
        accounts = await get_accounts(s, cb.from_user.id)
    if not accounts:
        await edit_or_send(cb,
            f"{ce(E_WARN,'⚠️')} No accounts added yet.",
            kb_back("accounts_menu"))
        await cb.answer()
        return
    lines = []
    for i, a in enumerate(accounts, 1):
        status = "🟢" if a.broadcast_running else "🔴"
        name = f"@{a.tg_username}" if a.tg_username else f"id:{a.tg_user_id}"
        lines.append(f"{i}. {status} <code>{name}</code>")
    await edit_or_send(cb,
        f"{ce(E_USER,'👤')} <b>Your Accounts</b>\n\n" + "\n".join(lines),
        kb_back("accounts_menu"))
    await cb.answer()


@router.callback_query(F.data == "health_check")
async def cb_health_check(cb: CallbackQuery):
    await cb.answer("Health check running…")
    async with get_db() as s:
        accounts = await get_accounts(s, cb.from_user.id)
    if not accounts:
        await cb.message.answer("No accounts to check.")
        return
    results = []
    for acct in accounts:
        ok = False
        if acct.session_string:
            try:
                cl = TelegramClient(StringSession(acct.session_string), TG_API_ID, TG_API_HASH)
                await asyncio.wait_for(cl.connect(), timeout=10)
                me2 = await asyncio.wait_for(cl.get_me(), timeout=10)
                ok = me2 is not None
                await cl.disconnect()
            except Exception:
                ok = False
        async with get_db() as s2:
            await s2.execute(
                update(HostedAccount)
                .where(HostedAccount.id == acct.id)
                .values(health_ok=ok, last_health_check=datetime.now(timezone.utc))
            )
            await s2.commit()
        icon = "✅" if ok else "❌"
        name = f"@{acct.tg_username}" if acct.tg_username else str(acct.tg_user_id)
        results.append(f"{icon} <code>{name}</code>")
    await cb.message.answer(
        f"{ce(E_EYE,'👀')} <b>Health Check</b>\n\n" + "\n".join(results),
        parse_mode="HTML", reply_markup=kb_back("accounts_menu")
    )


@router.callback_query(F.data == "warmup_menu")
async def cb_warmup_menu(cb: CallbackQuery):
    async with get_db() as s:
        accounts = await get_accounts(s, cb.from_user.id)
    if not accounts:
        await cb.answer("No accounts.", show_alert=True)
        return
    any_on = any(a.warmup_mode for a in accounts)
    async with get_db() as s:
        for a in accounts:
            until = datetime.now(timezone.utc) + timedelta(hours=24) if not any_on else None
            await s.execute(
                update(HostedAccount)
                .where(HostedAccount.id == a.id)
                .values(warmup_mode=not any_on, warmup_until=until)
            )
        await s.commit()
    status = "enabled" if not any_on else "disabled"
    await cb.answer(f"Warmup mode {status} for all accounts.", show_alert=True)


@router.callback_query(F.data == "remove_account_menu")
async def cb_remove_account_menu(cb: CallbackQuery):
    async with get_db() as s:
        accounts = await get_accounts(s, cb.from_user.id)
    if not accounts:
        await cb.answer("No accounts to remove.", show_alert=True)
        return
    rows = []
    for a in accounts:
        name = f"@{a.tg_username}" if a.tg_username else str(a.tg_user_id)
        rows.append([btn(f"❌ {name}", f"remove_acct_{a.id}")])
    rows.append([btn("◀️ Back", "accounts_menu")])
    await edit_or_send(cb,
        f"{ce(E_BAN,'🚫')} Select account to remove:",
        InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()


@router.callback_query(F.data.startswith("remove_acct_"))
async def cb_remove_acct(cb: CallbackQuery):
    acct_id = int(cb.data.split("_")[-1])
    async with get_db() as s:
        r = await s.execute(
            select(HostedAccount).where(
                HostedAccount.id == acct_id,
                HostedAccount.owner_id == cb.from_user.id,
            )
        )
        acct = r.scalar_one_or_none()
        if acct:
            acct.is_active = False
            await s.commit()
    await edit_or_send(cb, f"{ce(E_DONE,'✅')} Account removed.", kb_back("accounts_menu"))
    await cb.answer()


# ============================================================
# BROADCAST — set message
# ============================================================
@router.callback_query(F.data == "set_ad_msg")
async def cb_set_ad_msg(cb: CallbackQuery, state: FSMContext):
    await state.set_state(BroadcastSetup.waiting_message)
    await cb.message.answer(
        f"{ce(E_CHAT,'💬')} <b>Send your ad message:</b>\n"
        "HTML formatting supported. /cancel to abort.",
        parse_mode="HTML"
    )
    await cb.answer()


@router.message(BroadcastSetup.waiting_message)
async def handle_ad_msg(msg: Message, state: FSMContext):
    async with get_db() as s:
        cfg = await get_or_create_config(s, msg.from_user.id)
        cfg.ad_message = msg.text or msg.caption or ""
        cfg.updated_at = datetime.now(timezone.utc)
        await s.commit()
    await state.clear()
    await msg.answer(f"{ce(E_DONE,'✅')} <b>Ad message saved!</b>",
                     parse_mode="HTML", reply_markup=kb_back("bc_menu"))


# ── set media ─────────────────────────────────────────────────
@router.callback_query(F.data == "set_ad_media")
async def cb_set_ad_media(cb: CallbackQuery, state: FSMContext):
    await state.set_state(BroadcastSetup.waiting_media)
    await cb.message.answer(
        f"{ce(E_MONITOR,'🖼')} Send a photo or video for your ad.",
        parse_mode="HTML"
    )
    await cb.answer()


@router.message(BroadcastSetup.waiting_media, F.photo | F.video)
async def handle_ad_media(msg: Message, state: FSMContext):
    if msg.photo:
        media_id, media_type = msg.photo[-1].file_id, "photo"
    else:
        media_id, media_type = msg.video.file_id, "video"
    async with get_db() as s:
        cfg = await get_or_create_config(s, msg.from_user.id)
        cfg.ad_media_id = media_id
        cfg.ad_media_type = media_type
        await s.commit()
    await state.clear()
    await msg.answer(f"{ce(E_DONE,'✅')} Media saved ({media_type}).",
                     parse_mode="HTML", reply_markup=kb_back("bc_menu"))


# ============================================================
# BROADCAST LOOP
# ============================================================
async def broadcast_loop(owner_id: int):
    while True:
        try:
            async with get_db() as s:
                u = await get_or_create_user(s, owner_id)
                if not plan_is_active(u):
                    log.info("Plan expired for %s, stopping broadcast", owner_id)
                    break
                cfg = await get_or_create_config(s, owner_id)
                accounts = await get_accounts(s, owner_id)
                interval = cfg.interval_seconds
                delay = cfg.delay_seconds
                # A/B message selection
                if cfg.ab_active and cfg.ab_variant_b:
                    ad_msg = random.choice([cfg.ad_message, cfg.ab_variant_b])
                else:
                    ad_msg = cfg.ad_message
                target_raw = cfg.target_groups or ""

            targets = parse_group_links(target_raw)
            if not targets or not ad_msg:
                await asyncio.sleep(60)
                continue

            for acct in accounts:
                if not acct.broadcast_running or not acct.session_string:
                    continue
                if acct.warmup_mode:
                    continue
                try:
                    cl = TelegramClient(
                        StringSession(acct.session_string), TG_API_ID, TG_API_HASH
                    )
                    await asyncio.wait_for(cl.connect(), timeout=15)
                    for group in targets:
                        try:
                            await cl.send_message(group, ad_msg, parse_mode="html")
                        except FloodWaitError as e:
                            log.warning("Flood %ss on %s", e.seconds, group)
                            await asyncio.sleep(e.seconds)
                        except Exception as e:
                            log.warning("Send failed %s: %s", group, e)
                        await asyncio.sleep(delay)
                    await cl.disconnect()
                except Exception as e:
                    log.error("Account %s error: %s", acct.id, e)

            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.error("broadcast_loop error for %s: %s", owner_id, e)
            await asyncio.sleep(60)


@router.callback_query(F.data == "start_broadcast")
async def cb_start_broadcast(cb: CallbackQuery):
    async with get_db() as s:
        cfg = await get_or_create_config(s, cb.from_user.id)
        accounts = await get_accounts(s, cb.from_user.id)
        if not cfg.ad_message:
            await cb.answer("❌ Set your ad message first.", show_alert=True)
            return
        if not accounts:
            await cb.answer("❌ Add an account first.", show_alert=True)
            return
        for a in accounts:
            a.broadcast_running = True
        await s.commit()
    owner_id = cb.from_user.id
    if owner_id in _broadcast_tasks:
        _broadcast_tasks[owner_id].cancel()
    _broadcast_tasks[owner_id] = asyncio.create_task(broadcast_loop(owner_id))
    await edit_or_send(cb,
        f"{ce(E_BROADCAST,'📢')} <b>Broadcast started!</b>",
        kb_back("bc_menu"))
    await cb.answer()


@router.callback_query(F.data == "stop_broadcast")
async def cb_stop_broadcast(cb: CallbackQuery):
    owner_id = cb.from_user.id
    if owner_id in _broadcast_tasks:
        _broadcast_tasks[owner_id].cancel()
        del _broadcast_tasks[owner_id]
    async with get_db() as s:
        await s.execute(
            update(HostedAccount)
            .where(HostedAccount.owner_id == owner_id)
            .values(broadcast_running=False)
        )
        await s.commit()
    await edit_or_send(cb,
        f"{ce(E_CROSS,'❌')} <b>Broadcast stopped.</b>",
        kb_back("bc_menu"))
    await cb.answer()


# ============================================================
# CONFIG HANDLERS
# ============================================================
@router.callback_query(F.data == "cfg_interval")
async def cb_cfg_interval(cb: CallbackQuery, state: FSMContext):
    async with get_db() as s:
        u = await get_or_create_user(s, cb.from_user.id)
    await state.set_state(UserConfig.waiting_interval)
    await cb.message.answer(
        f"{ce(E_CLOCK,'⌛')} <b>Set Interval</b>\n"
        f"Your plan minimum: <b>{plan_interval_min(u.plan)} minutes</b>\n"
        "Send number of minutes:",
        parse_mode="HTML"
    )
    await cb.answer()


@router.message(UserConfig.waiting_interval)
async def handle_interval(msg: Message, state: FSMContext):
    try:
        mins = int(msg.text.strip())
    except ValueError:
        await msg.answer("❌ Please send a number.")
        return
    async with get_db() as s:
        u = await get_or_create_user(s, msg.from_user.id)
        min_m = plan_interval_min(u.plan)
        if mins < min_m:
            await msg.answer(f"⚠️ Minimum is <b>{min_m}m</b> for your plan.",
                             parse_mode="HTML")
            return
        cfg = await get_or_create_config(s, msg.from_user.id)
        cfg.interval_seconds = mins * 60
        await s.commit()
    await state.clear()
    await msg.answer(f"{ce(E_DONE,'✅')} Interval set to <b>{mins} minutes</b>.",
                     parse_mode="HTML", reply_markup=kb_back("config_menu"))


@router.callback_query(F.data == "cfg_delay")
async def cb_cfg_delay(cb: CallbackQuery, state: FSMContext):
    await state.set_state(UserConfig.waiting_delay)
    await cb.message.answer("Send delay between messages (seconds):")
    await cb.answer()


@router.message(UserConfig.waiting_delay)
async def handle_delay(msg: Message, state: FSMContext):
    try:
        secs = int(msg.text.strip())
    except ValueError:
        await msg.answer("❌ Please send a number.")
        return
    async with get_db() as s:
        cfg = await get_or_create_config(s, msg.from_user.id)
        cfg.delay_seconds = secs
        await s.commit()
    await state.clear()
    await msg.answer(f"{ce(E_DONE,'✅')} Delay set to <b>{secs}s</b>.",
                     parse_mode="HTML", reply_markup=kb_back("config_menu"))


@router.callback_query(F.data == "cfg_groups")
async def cb_cfg_groups(cb: CallbackQuery, state: FSMContext):
    await state.set_state(UserConfig.waiting_groups)
    await cb.message.answer(
        f"{ce(E_ADD,'➕')} <b>Target Groups</b>\n\n"
        "Send group links or @usernames, one per line:",
        parse_mode="HTML"
    )
    await cb.answer()


@router.message(UserConfig.waiting_groups)
async def handle_groups(msg: Message, state: FSMContext):
    links = parse_group_links(msg.text)
    async with get_db() as s:
        cfg = await get_or_create_config(s, msg.from_user.id)
        cfg.target_groups = " ".join(links)
        await s.commit()
    await state.clear()
    await msg.answer(f"{ce(E_DONE,'✅')} <b>{len(links)} group(s) saved.</b>",
                     parse_mode="HTML", reply_markup=kb_back("config_menu"))


@router.callback_query(F.data == "cfg_reply")
async def cb_cfg_reply(cb: CallbackQuery, state: FSMContext):
    await state.set_state(UserConfig.waiting_reply_msg)
    await cb.message.answer("Send auto-reply message (or 'off' to disable):")
    await cb.answer()


@router.message(UserConfig.waiting_reply_msg)
async def handle_reply_msg(msg: Message, state: FSMContext):
    text = msg.text.strip()
    async with get_db() as s:
        cfg = await get_or_create_config(s, msg.from_user.id)
        if text.lower() == "off":
            cfg.auto_reply_on = False
        else:
            cfg.auto_reply_msg = text
            cfg.auto_reply_on = True
        await s.commit()
    await state.clear()
    status = "enabled" if text.lower() != "off" else "disabled"
    await msg.answer(f"{ce(E_DONE,'✅')} Auto-reply {status}.",
                     parse_mode="HTML", reply_markup=kb_back("config_menu"))


@router.callback_query(F.data == "cfg_log_group")
async def cb_cfg_log_group(cb: CallbackQuery, state: FSMContext):
    await state.set_state(UserConfig.waiting_log_group)
    await cb.message.answer(
        f"{ce(E_TOP,'📌')} <b>Log Group</b>\n\n"
        "Forward a message from your log group, or send its ID.\n"
        "Bot must be admin in that group.",
        parse_mode="HTML"
    )
    await cb.answer()


@router.message(UserConfig.waiting_log_group)
async def handle_log_group(msg: Message, state: FSMContext):
    group_id = None
    if msg.forward_from_chat:
        group_id = msg.forward_from_chat.id
    else:
        try:
            group_id = int(msg.text.strip())
        except ValueError:
            pass
    if not group_id:
        await msg.answer("❌ Could not parse group. Try forwarding a message from it.")
        return
    async with get_db() as s:
        u = await get_or_create_user(s, msg.from_user.id)
        u.log_group_id = group_id
        await s.commit()
    await state.clear()
    await msg.answer(f"{ce(E_DONE,'✅')} Log group set.",
                     parse_mode="HTML", reply_markup=kb_back("config_menu"))


@router.callback_query(F.data == "cfg_notify")
async def cb_cfg_notify(cb: CallbackQuery):
    async with get_db() as s:
        u = await get_or_create_user(s, cb.from_user.id)
        u.notify_on = not u.notify_on
        await s.commit()
        status = "ON" if u.notify_on else "OFF"
    await cb.answer(f"Notifications {status}", show_alert=True)


@router.callback_query(F.data == "cfg_join")
async def cb_cfg_join(cb: CallbackQuery):
    async with get_db() as s:
        cfg = await get_or_create_config(s, cb.from_user.id)
        cfg.auto_join_on = not cfg.auto_join_on
        await s.commit()
        status = "ON" if cfg.auto_join_on else "OFF"
    await cb.answer(f"Auto-join {status}", show_alert=True)


@router.callback_query(F.data == "cfg_categories")
async def cb_cfg_categories(cb: CallbackQuery):
    await cb.answer("Tag groups by category when setting target groups.", show_alert=True)


@router.callback_query(F.data == "cfg_clean_dead")
async def cb_clean_dead(cb: CallbackQuery):
    await cb.answer("Dead group cleaner will scan on next broadcast cycle.", show_alert=True)


# ============================================================
# TEMPLATES
# ============================================================
@router.callback_query(F.data == "templates_menu")
async def cb_templates_menu(cb: CallbackQuery):
    async with get_db() as s:
        r = await s.execute(
            select(AdTemplate).where(AdTemplate.owner_id == cb.from_user.id)
        )
        templates = r.scalars().all()
    lines = [f"{i+1}. <code>{t.name}</code>" for i, t in enumerate(templates)]
    text = (
        (f"{ce(E_CHAT,'💬')} <b>Ad Templates</b>\n\n" + "\n".join(lines))
        if templates else
        f"{ce(E_WARN,'⚠️')} No templates saved yet."
    )
    rows = [
        [btn("➕ New Template", "template_new")],
        [btn("◀️ Back", "bc_menu")],
    ]
    await edit_or_send(cb, text, InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()


@router.callback_query(F.data == "template_new")
async def cb_template_new(cb: CallbackQuery, state: FSMContext):
    await state.set_state(UserConfig.waiting_template_name)
    await cb.message.answer("Send a name for this template:")
    await cb.answer()


@router.message(UserConfig.waiting_template_name)
async def handle_template_name(msg: Message, state: FSMContext):
    await state.update_data(tpl_name=msg.text.strip())
    await state.set_state(UserConfig.waiting_template_msg)
    await msg.answer("Now send the ad message for this template:")


@router.message(UserConfig.waiting_template_msg)
async def handle_template_msg(msg: Message, state: FSMContext):
    data = await state.get_data()
    async with get_db() as s:
        s.add(AdTemplate(owner_id=msg.from_user.id, name=data["tpl_name"],
                         message=msg.text or ""))
        await s.commit()
    await state.clear()
    await msg.answer(f"{ce(E_DONE,'✅')} Template saved.",
                     parse_mode="HTML", reply_markup=kb_back("templates_menu"))


# ============================================================
# A/B TESTING
# ============================================================
@router.callback_query(F.data == "ab_menu")
async def cb_ab_menu(cb: CallbackQuery):
    async with get_db() as s:
        cfg = await get_or_create_config(s, cb.from_user.id)
    status = "ON" if cfg.ab_active else "OFF"
    rows = [
        [btn("✏️ Set Variant B", "ab_set_b")],
        [btn(f"🔀 Toggle A/B ({status})", "ab_toggle")],
        [btn("◀️ Back", "bc_menu")],
    ]
    await edit_or_send(cb,
        f"{ce(E_LIGHTNING,'⚡')} <b>A/B Testing — {status}</b>",
        InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()


@router.callback_query(F.data == "ab_toggle")
async def cb_ab_toggle(cb: CallbackQuery):
    async with get_db() as s:
        cfg = await get_or_create_config(s, cb.from_user.id)
        cfg.ab_active = not cfg.ab_active
        await s.commit()
        status = "ON" if cfg.ab_active else "OFF"
    await cb.answer(f"A/B testing {status}", show_alert=True)


@router.callback_query(F.data == "ab_set_b")
async def cb_ab_set_b(cb: CallbackQuery, state: FSMContext):
    # Reuse broadcast message state but store in ab_variant_b
    await state.set_state(BroadcastSetup.waiting_message)
    await state.update_data(ab_mode=True)
    await cb.message.answer("Send Variant B message:")
    await cb.answer()


# ============================================================
# SCHEDULER
# ============================================================
@router.callback_query(F.data == "scheduler_menu")
async def cb_scheduler_menu(cb: CallbackQuery):
    await edit_or_send(cb,
        f"{ce(E_CLOCK,'⌛')} <b>Scheduler</b>\n\nSchedule broadcasts start/stop by date.",
        kb_back("bc_menu"))
    await cb.answer()


# ============================================================
# ADMIN PANEL
# ============================================================
@router.callback_query(F.data == "admin_panel")
async def cb_admin_panel(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Unauthorized.", show_alert=True)
        return
    await edit_or_send(cb,
        f"{ce(E_KING,'👑')} <b>Admin Panel</b>",
        kb_admin_panel())
    await cb.answer()


@router.callback_query(F.data == "admin_gen_key")
async def cb_admin_gen_key(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        await cb.answer("Unauthorized.", show_alert=True)
        return
    await state.set_state(AdminAction.waiting_key_plan)
    rows = [[btn(f"{v['label']}", f"akplan_{k}")]
            for k, v in PLAN_CONFIG.items() if k != "free"]
    rows.append([btn("◀️ Cancel", "admin_panel")])
    await edit_or_send(cb, "Select plan for new key:",
                       InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()


@router.callback_query(F.data.startswith("akplan_"))
async def cb_akplan(cb: CallbackQuery, state: FSMContext):
    plan = cb.data.split("_", 1)[1]
    await state.update_data(key_plan=plan)
    await state.set_state(AdminAction.waiting_key_duration)
    await cb.message.answer(f"Plan: <b>{plan}</b>\nSend duration in days:", parse_mode="HTML")
    await cb.answer()


@router.message(AdminAction.waiting_key_duration)
async def handle_key_duration(msg: Message, state: FSMContext):
    try:
        days = int(msg.text.strip())
    except ValueError:
        await msg.answer("❌ Send a number.")
        return
    await state.update_data(key_duration=days)
    await state.set_state(AdminAction.waiting_key_count)
    await msg.answer("How many keys to generate? (1–50)")


@router.message(AdminAction.waiting_key_count)
async def handle_key_count(msg: Message, state: FSMContext):
    try:
        count = max(1, min(50, int(msg.text.strip())))
    except ValueError:
        await msg.answer("❌ Send a number.")
        return
    data = await state.get_data()
    plan = data["key_plan"]
    days = data["key_duration"]
    keys = []
    async with get_db() as s:
        for _ in range(count):
            k = gen_key(6)
            s.add(AccessKey(key=k, plan=plan, duration_days=days,
                            created_by=msg.from_user.id))
            keys.append(k)
        await s.commit()
        await add_audit(s, msg.from_user.id, "gen_keys",
                        details=f"{count}x {plan} {days}d")
    await state.clear()
    key_list = "\n".join(f"<code>{k}</code>" for k in keys)
    await msg.answer(
        f"{ce(E_DONE,'✅')} <b>{count} key(s) generated</b>\nPlan: {plan} / {days}d\n\n"
        + key_list,
        parse_mode="HTML"
    )


@router.callback_query(F.data == "admin_list_keys")
async def cb_admin_list_keys(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Unauthorized.", show_alert=True)
        return
    async with get_db() as s:
        r = await s.execute(
            select(AccessKey).where(AccessKey.used == False)
            .order_by(AccessKey.created_at.desc())
        )
        keys = r.scalars().all()
    if not keys:
        await cb.answer("No unused keys.", show_alert=True)
        return
    lines = [f"<code>{k.key}</code> — {k.plan}/{k.duration_days}d" for k in keys[:30]]
    await cb.message.answer(
        f"{ce(E_GOLD,'⭐')} <b>Unused Keys ({len(keys)})</b>\n\n" + "\n".join(lines),
        parse_mode="HTML"
    )
    await cb.answer()


@router.callback_query(F.data == "admin_revoke_key")
async def cb_admin_revoke_key(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        await cb.answer("Unauthorized.", show_alert=True)
        return
    await state.set_state(AdminAction.waiting_key_revoke)
    await cb.message.answer("Send the key to revoke:")
    await cb.answer()


@router.message(AdminAction.waiting_key_revoke)
async def handle_key_revoke(msg: Message, state: FSMContext):
    key_str = msg.text.strip().upper()
    async with get_db() as s:
        r = await s.execute(select(AccessKey).where(AccessKey.key == key_str))
        ak = r.scalar_one_or_none()
        if not ak:
            await msg.answer("❌ Key not found.")
            await state.clear()
            return
        ak.used = True
        ak.used_at = datetime.now(timezone.utc)
        await s.commit()
        await add_audit(s, msg.from_user.id, "revoke_key", target=key_str)
    await state.clear()
    await msg.answer(f"{ce(E_DONE,'✅')} Key <code>{key_str}</code> revoked.",
                     parse_mode="HTML")


@router.callback_query(F.data == "admin_users")
async def cb_admin_users(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Unauthorized.", show_alert=True)
        return
    async with get_db() as s:
        r = await s.execute(select(LazUser).order_by(LazUser.created_at.desc()))
        users = r.scalars().all()
    lines = []
    for u in users[:25]:
        name = u.username or str(u.id)
        lines.append(f"@{name} — <b>{u.plan}</b>")
    await cb.message.answer(
        f"{ce(E_USER,'👤')} <b>Users ({len(users)})</b>\n\n" + "\n".join(lines),
        parse_mode="HTML", reply_markup=kb_back("admin_panel")
    )
    await cb.answer()


@router.callback_query(F.data == "admin_force_stop")
async def cb_admin_force_stop(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Unauthorized.", show_alert=True)
        return
    for task in list(_broadcast_tasks.values()):
        task.cancel()
    _broadcast_tasks.clear()
    async with get_db() as s:
        await s.execute(update(HostedAccount).values(broadcast_running=False))
        await s.commit()
        await add_audit(s, cb.from_user.id, "force_stop_all")
    await cb.answer("All broadcasts force-stopped.", show_alert=True)


@router.callback_query(F.data == "admin_announce")
async def cb_admin_announce(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        await cb.answer("Unauthorized.", show_alert=True)
        return
    await state.set_state(AdminAction.waiting_announce)
    await cb.message.answer("Send announcement message to all users:")
    await cb.answer()


@router.message(AdminAction.waiting_announce)
async def handle_announce(msg: Message, state: FSMContext):
    text = msg.text or ""
    async with get_db() as s:
        r = await s.execute(select(LazUser))
        users = r.scalars().all()
    sent = 0
    for u in users:
        try:
            await bot_instance.send_message(
                u.id, f"📢 <b>Announcement</b>\n\n{text}", parse_mode="HTML"
            )
            sent += 1
        except Exception:
            pass
        await asyncio.sleep(0.05)
    await state.clear()
    async with get_db() as s:
        await add_audit(s, msg.from_user.id, "announce", details=f"sent to {sent}")
    await msg.answer(f"{ce(E_DONE,'✅')} Sent to {sent} users.", parse_mode="HTML")


@router.callback_query(F.data == "admin_stats")
async def cb_admin_stats(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Unauthorized.", show_alert=True)
        return
    async with get_db() as s:
        users = (await s.execute(select(LazUser))).scalars().all()
        accts = (await s.execute(
            select(HostedAccount).where(HostedAccount.is_active == True)
        )).scalars().all()
        keys = (await s.execute(select(AccessKey))).scalars().all()
    plan_counts: dict = {}
    for u in users:
        plan_counts[u.plan] = plan_counts.get(u.plan, 0) + 1
    plan_lines = "\n".join(f"  {p}: {c}" for p, c in plan_counts.items())
    text = (
        f"{ce(E_GOLD,'⭐')} <b>Bot Stats</b>\n\n"
        f"Total users: <b>{len(users)}</b>\n"
        f"Active accounts: <b>{len(accts)}</b>\n"
        f"Keys total/used: <b>{len(keys)}/{sum(1 for k in keys if k.used)}</b>\n"
        f"Active broadcasts: <b>{len(_broadcast_tasks)}</b>\n\n"
        f"Plans:\n{plan_lines}"
    )
    await cb.message.answer(text, parse_mode="HTML", reply_markup=kb_back("admin_panel"))
    await cb.answer()


@router.callback_query(F.data == "admin_audit")
async def cb_admin_audit(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Unauthorized.", show_alert=True)
        return
    async with get_db() as s:
        r = await s.execute(select(AuditLog).order_by(AuditLog.created_at.desc()))
        logs = r.scalars().all()
    lines = [
        f"<code>{l.created_at.strftime('%m-%d %H:%M')}</code> {l.action} {l.target or ''}"
        for l in logs[:20]
    ]
    await cb.message.answer(
        f"{ce(E_SCAN,'📜')} <b>Audit Log</b>\n\n" + "\n".join(lines),
        parse_mode="HTML", reply_markup=kb_back("admin_panel")
    )
    await cb.answer()


@router.callback_query(F.data == "admin_bot_config")
async def cb_admin_bot_config(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Unauthorized.", show_alert=True)
        return
    async with get_db() as s:
        maint = await get_setting(s, "maintenance", "0")
    rows = [
        [btn(f"🔧 Maintenance: {'ON' if maint=='1' else 'OFF'}", "admin_toggle_maint")],
        [btn("◀️ Back", "admin_panel")],
    ]
    await edit_or_send(cb, f"{ce(E_SETTINGS,'⚙️')} <b>Bot Config</b>",
                       InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()


@router.callback_query(F.data == "admin_toggle_maint")
async def cb_admin_toggle_maint(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Unauthorized.", show_alert=True)
        return
    async with get_db() as s:
        cur = await get_setting(s, "maintenance", "0")
        await set_setting(s, "maintenance", "0" if cur == "1" else "1")
        await add_audit(s, cb.from_user.id, "toggle_maintenance")
    await cb.answer("Maintenance toggled.", show_alert=True)


@router.callback_query(F.data == "admin_backup")
async def cb_admin_backup(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Unauthorized.", show_alert=True)
        return
    await cb.answer("Use Neon dashboard for DB backups.", show_alert=True)


# ============================================================
# BACKGROUND — PLAN EXPIRY WATCHER
# ============================================================
async def plan_expiry_watcher():
    while True:
        try:
            now = datetime.now(timezone.utc)
            async with get_db() as s:
                r = await s.execute(
                    select(LazUser).where(
                        LazUser.plan != "free",
                        LazUser.plan_expires.isnot(None),
                    )
                )
                users = r.scalars().all()
                for u in users:
                    if not u.plan_expires:
                        continue
                    exp = u.plan_expires.replace(tzinfo=timezone.utc)
                    days_left = (exp - now).days
                    if days_left <= 0:
                        old_plan = u.plan
                        u.plan = "free"
                        u.plan_expires = None
                        await s.commit()
                        try:
                            if bot_instance:
                                await bot_instance.send_message(
                                    u.id,
                                    f"{ce(E_WARN,'⚠️')} <b>Plan Expired</b>\n\n"
                                    f"Your <b>{old_plan}</b> plan has expired.\n"
                                    f"Renew at {SUBSCRIPTION}",
                                    parse_mode="HTML"
                                )
                        except Exception:
                            pass
                    elif days_left <= 3 and u.notify_on:
                        try:
                            if bot_instance:
                                await bot_instance.send_message(
                                    u.id,
                                    f"{ce(E_ALERT,'🚨')} <b>Plan expiring in {days_left} day(s)!</b>\n"
                                    f"Renew: {SUBSCRIPTION}",
                                    parse_mode="HTML"
                                )
                        except Exception:
                            pass
        except Exception as e:
            log.error("plan_expiry_watcher: %s", e)
        await asyncio.sleep(3600)


# ============================================================
# BACKGROUND — WEEKLY REPORT
# ============================================================
async def weekly_report_sender():
    await asyncio.sleep(600)
    while True:
        try:
            async with get_db() as s:
                r = await s.execute(select(LazUser).where(LazUser.notify_on == True))
                users = r.scalars().all()
                for u in users:
                    accounts = await get_accounts(s, u.id)
                    running = sum(1 for a in accounts if a.broadcast_running)
                    try:
                        if bot_instance:
                            await bot_instance.send_message(
                                u.id,
                                f"{ce(E_GOLD,'📊')} <b>Weekly Report</b>\n\n"
                                f"Accounts: <b>{len(accounts)}</b>\n"
                                f"Running: <b>{running}</b>\n"
                                f"Plan: <b>{u.plan}</b>",
                                parse_mode="HTML"
                            )
                    except Exception:
                        pass
                    await asyncio.sleep(0.05)
        except Exception as e:
            log.error("weekly_report: %s", e)
        await asyncio.sleep(7 * 24 * 3600)


# ============================================================
# MAIN
# ============================================================
async def main():
    global bot_instance
    await init_db()

    bot_instance = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()
    dp.include_router(router)

    asyncio.create_task(plan_expiry_watcher())
    asyncio.create_task(weekly_report_sender())

    log.info("LazAdsBot v2 starting…")
    await dp.start_polling(bot_instance, allowed_updates=["message", "callback_query"])


if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    asyncio.run(main())
