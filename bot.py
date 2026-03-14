import os
import re
import html
import time
import random
import string
import logging
import datetime
import threading

import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from telebot.apihelper import ApiTelegramException

from database import (
    setup_db, get_user, update_user, add_referral,
    redeem_code, is_banned, ban_user, unban_user,
    get_all_users, create_code, give_all_credits,
    is_query_locked, add_lock, remove_lock, log_search,
    get_user_history, get_locked_list, add_credits_to_user,
    get_all_users_detail, update_user_info, get_today_search_count,
    get_search_stats_by_mode, get_search_stats_by_country,
    get_total_search_count, deduct_credit_atomic, get_code_info,
    db_pool,
)
from api import perform_lookup, detect_country

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  LOGGING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("lacera.bot")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CONFIG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN env var not set!")

OWNER_IDS: list[int] = [
    int(x.strip())
    for x in os.getenv("OWNER_ID", "").split(",")
    if x.strip().isdigit() and int(x.strip()) > 0
]
if not OWNER_IDS:
    logger.warning("⚠️  OWNER_ID not set — bot has zero admins!")

ADMIN_USERNAME    = os.getenv("ADMIN_USERNAME", "dissector007bot").lstrip("@")
REQUIRED_CHANNELS = [
    ch.strip()
    for ch in os.getenv("REQUIRED_CHANNELS", "@MindRupture,@laceraOsint").split(",")
    if ch.strip()
]

DAILY_LIMIT      = max(1,  int(os.getenv("DAILY_LIMIT",      "4")))
COOLDOWN_SECONDS = max(0,  int(os.getenv("COOLDOWN_SECONDS", "5")))
AUTO_DELETE_SECS = max(30, int(os.getenv("AUTO_DELETE_SECS", "120")))

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ALLOWED COUNTRIES — strict whitelist
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Only these 3 country prefixes are allowed for number search
ALLOWED_CC: dict[str, tuple[str, str]] = {
    "91":  ("🇮🇳", "India"),
    "92":  ("🇵🇰", "Pakistan"),
    "880": ("🇧🇩", "Bangladesh"),
}

# ── UI constants ──
DIV       = "─" * 26
WATERMARK = (
    "\n\n"
    f"<i>{'·' * 24}</i>\n"
    "⚡ <b>ʟᴀᴄᴇʀᴀ ᴏsɪɴᴛ</b>  ·  ᴘʀᴇᴍɪᴜᴍ ɪɴᴛᴇʟ\n"
    f"<a href='https://t.me/NeuroLacera'>@ɴᴇᴜʀᴏʟᴀᴄᴇʀᴀ</a>  ·  "
    f"<a href='https://t.me/LaceraOsintBot'>@ʟᴀᴄᴇʀᴀᴏsɪɴᴛʙᴏᴛ</a>"
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  STATE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_shutdown_event  = threading.Event()
BOT_START_TIME   = time.time()

cash_reports:    dict = {}
_cash_lock       = threading.Lock()

USER_COOLDOWN:   dict = {}
_cooldown_lock   = threading.Lock()

_join_cache:     dict = {}
_join_cache_lock = threading.Lock()
_JOIN_CACHE_TTL  = 120

_delete_sema     = threading.Semaphore(50)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  DB + BOT INIT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

setup_db()
if not db_pool:
    raise RuntimeError("DB pool failed to init — check DATABASE_URL")

bot = telebot.TeleBot(
    BOT_TOKEN,
    parse_mode="HTML",
    threaded=True,
    num_threads=8,
)

try:
    _BOT_USERNAME = bot.get_me().username or "LaceraOsintBot"
except Exception as _e:
    _BOT_USERNAME = "LaceraOsintBot"
    logger.warning("get_me() failed, using fallback: %s", _e)
logger.info("Bot username: @%s", _BOT_USERNAME)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PHONE NORMALIZATION — 3 countries only
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def normalize_phone(raw: str) -> tuple[str, str]:
    """
    Normalize phone → (e164_digits_no_plus, cc_str) or ("", "").
    Allowed: India (+91, 10-digit bare), Pakistan (+92), Bangladesh (+880).
    Rejects all other country codes.
    API gets digits WITHOUT plus sign e.g. 919876543210
    """
    if not raw or not isinstance(raw, str) or len(raw) > 20:
        return ("", "")

    raw_s    = raw.strip()
    has_plus = raw_s.startswith("+")
    clean    = re.sub(r"[^\d]", "", raw_s)

    # 00-prefix → strip 00, treat as international
    if not has_plus and clean.startswith("00") and len(clean) > 4:
        clean = clean[2:]
        has_plus = True

    # 0-prefix 11-digit → India local format (MUST check before international block)
    if not has_plus and len(clean) == 11 and clean.startswith("0") and clean[1:].isdigit():
        return (f"91{clean[1:]}", "91")

    if has_plus or (not has_plus and len(clean) > 10):
        # International format — check allowed prefix
        digits = clean
        # Try longest prefix first (880 before 88, etc.)
        for pfx in sorted(ALLOWED_CC.keys(), key=len, reverse=True):
            if digits.startswith(pfx):
                national = digits[len(pfx):]
                # Basic length sanity: national part should be 7-11 digits
                if not national.isdigit() or not (7 <= len(national) <= 11):
                    return ("", "")
                return (digits, pfx)
        # Unknown/blocked country code
        return ("", "")

    # Bare number — assume India only
    if clean.isdigit():
        if len(clean) == 10:
            return (f"91{clean}", "91")
    return ("", "")


def get_allowed_cc_info(cc: str) -> tuple[str, str]:
    """Returns (flag, country_name) for allowed cc, else ('🌍', 'Unknown')."""
    return ALLOWED_CC.get(cc, ("🌍", "Unknown"))

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  INPUT VALIDATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def validate_query(q: str, mode: str) -> tuple[str, str | None]:
    """Returns (sanitized_q, error_msg|None)."""
    if len(q) > 200:
        return ("", "❌  ɪɴᴘᴜᴛ ʙʜᴀɪ, ᴢʏᴀᴅᴀ ʟᴀᴍʙᴀ ɴᴀ ᴅᴏ.")

    if mode == "email":
        if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", q):
            return ("", "❌  <b>ɪɴᴠᴀʟɪᴅ ᴇᴍᴀɪʟ</b>  ›  <code>user@domain.com</code>")

    elif mode == "aadhar":
        digits = re.sub(r"\D", "", q)
        if len(digits) != 12:
            return ("", "❌  <b>ɪɴᴠᴀʟɪᴅ ᴀᴀᴅʜᴀᴀʀ</b>  ›  12 ᴅɪɢɪᴛs ᴄʜᴀʜɪʏᴇ")
        return (digits, None)

    elif mode == "pan":
        pan = q.strip().upper()
        if not re.match(r"^[A-Z]{5}[0-9]{4}[A-Z]$", pan):
            return ("", "❌  <b>ɪɴᴠᴀʟɪᴅ ᴘᴀɴ</b>  ›  <code>ABCDE1234F</code>")
        return (pan, None)

    elif mode == "vehicle":
        v = q.strip().upper().replace(" ", "")
        if not re.match(r"^[A-Z]{2}[0-9]{1,2}[A-Z]{1,3}[0-9]{4}$", v):
            return ("", "❌  <b>ɪɴᴠᴀʟɪᴅ ᴠᴇʜɪᴄʟᴇ</b>  ›  <code>MH12AB1234</code>")
        return (v, None)

    elif mode == "ip":
        parts_ip = q.strip().split(".")
        if len(parts_ip) != 4:
            return ("", "❌  <b>ɪɴᴠᴀʟɪᴅ ɪᴘ</b>  ›  <code>1.2.3.4</code>")
        try:
            octets = [int(p) for p in parts_ip]
            if not all(0 <= o <= 255 for o in octets):
                raise ValueError
        except ValueError:
            return ("", "❌  <b>ɪɴᴠᴀʟɪᴅ ɪᴘ</b>  ›  <code>1.2.3.4</code>")
        a, b = octets[0], octets[1]
        if a in (0, 10, 127) or (a == 172 and 16 <= b <= 31) or (a == 192 and b == 168):
            return ("", "❌  ᴘʀɪᴠᴀᴛᴇ / ʟᴏᴄᴀʟ ɪᴘ ɴᴀʜɪ ᴄʜᴀʟᴇɢᴀ.")

    return (q.strip(), None)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  UTILITIES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def is_admin(uid: int) -> bool:
    return uid in OWNER_IDS


def fmt_uptime(sec: float) -> str:
    sec = int(sec)
    d, r = divmod(sec, 86400)
    h, r = divmod(r, 3600)
    m, s = divmod(r, 60)
    parts = []
    if d: parts.append(f"{d}d")
    if h: parts.append(f"{h}h")
    if m: parts.append(f"{m}m")
    parts.append(f"{s}s")
    return " ".join(parts)


def fmt_reset(ts) -> str:
    if not ts:
        return "N/A"
    try:
        left = int(ts) - int(time.time())
    except (TypeError, ValueError):
        return "N/A"
    if left <= 0:
        return "ʀᴇsᴇᴛᴛɪɴɢ..."
    h, r = divmod(left, 3600)
    m, s = divmod(r, 60)
    return f"{h}h {m}m {s}s"


def fmt_bar(used: int, total: int, width: int = 10) -> str:
    used      = max(0, min(used, total))
    pct       = int((used / total) * 100) if total else 0
    filled_w  = int((used / total) * width) if total else 0
    filled    = "█" * filled_w
    empty     = "░" * (width - filled_w)
    return f"[{filled}{empty}] {pct}%"


def fmt_expiry(ts) -> str:
    if ts is None:
        return "ɴᴇᴠᴇʀ"
    try:
        left = int(ts) - int(time.time())
    except (TypeError, ValueError):
        return "N/A"
    if left <= 0:
        return "ᴇxᴘɪʀᴇᴅ ✗"
    if left < 60:
        return f"{left}s"
    if left < 3600:
        return f"{left // 60}m {left % 60}s"
    if left < 86400:
        return f"{left // 3600}h {(left % 3600) // 60}m"
    return f"{left // 86400}d {(left % 86400) // 3600}h"


def fmt_ts(ts) -> str:
    if not ts:
        return "N/A"
    try:
        return datetime.datetime.fromtimestamp(int(ts)).strftime("%d/%m/%y %H:%M")
    except (OSError, OverflowError, ValueError):
        return "N/A"


def parse_duration(raw: str) -> int | None:
    raw = raw.strip().lower()
    if not raw:
        return None
    total, found = 0, False
    for num_str, unit in re.findall(r"(\d+)\s*([smhd]?)", raw):
        if not num_str:
            continue
        n = int(num_str)
        if n == 0:
            continue
        found = True
        if unit == "s":   total += n
        elif unit == "m": total += n * 60
        elif unit == "h": total += n * 3600
        elif unit == "d": total += n * 86400
        else:             total += n * 60
    return min(total, 365 * 86400) if (found and total > 0) else None


def auto_delete_with_warning(chat_id: int, msg_id: int, delay: int = AUTO_DELETE_SECS) -> None:
    if delay <= 0:
        return
    warn_id = None
    try:
        warn_id = bot.send_message(
            chat_id,
            f"⏳  <i>ʀᴇsᴜʟᴛ  <b>{delay}s</b>  ᴍᴇ ᴅᴇʟᴇᴛᴇ ʜᴏ ᴊᴀᴀᴇɢᴀ</i>",
        ).message_id
    except Exception:
        pass
    time.sleep(delay)
    for mid in filter(None, [msg_id, warn_id]):
        try:
            bot.delete_message(chat_id, mid)
        except Exception:
            pass


def _spawn_delete(chat_id: int, msg_id: int) -> None:
    def _run():
        with _delete_sema:
            auto_delete_with_warning(chat_id, msg_id, AUTO_DELETE_SECS)
    threading.Thread(target=_run, daemon=True, name=f"del_{msg_id}").start()


def cache_cleanup() -> None:
    while True:
        time.sleep(300)
        now = time.time()

        with _cash_lock:
            dead = [k for k, v in list(cash_reports.items())
                    if isinstance(v, dict) and now - v.get("ts", now) > 600]
            for k in dead:
                cash_reports.pop(k, None)
        if dead:
            logger.info("[CACHE] cleared %d expired reports", len(dead))

        with _cooldown_lock:
            stale = [k for k, v in list(USER_COOLDOWN.items()) if now - v > 3600]
            for k in stale:
                USER_COOLDOWN.pop(k, None)

        with _join_cache_lock:
            stale_jc = [k for k, (_, ts) in list(_join_cache.items())
                        if now - ts > _JOIN_CACHE_TTL * 5]
            for k in stale_jc:
                _join_cache.pop(k, None)


threading.Thread(target=cache_cleanup, daemon=True, name="cache_cleanup").start()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SAFE TELEGRAM WRAPPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _parse_retry_after(exc_str: str, default: int = 5) -> int:
    try:
        return max(1, int(str(exc_str).split("retry after ")[-1].split()[0]))
    except Exception:
        return default


def safe_reply_to(message, text: str, **kwargs):
    try:
        return bot.reply_to(message, text, **kwargs)
    except ApiTelegramException as exc:
        if exc.error_code == 400:
            if "message is too long" in str(exc):
                text = re.sub(r"<[^>]+>", "", text)[:3800]
            try:
                return bot.send_message(message.chat.id, text, **kwargs)
            except Exception:
                pass
        elif exc.error_code == 429:
            time.sleep(_parse_retry_after(str(exc)))
            try:
                return bot.reply_to(message, text, **kwargs)
            except Exception:
                pass
        elif exc.error_code in (502, 503, 504):
            time.sleep(3)
            try:
                return bot.send_message(message.chat.id, text, **kwargs)
            except Exception:
                pass
        else:
            logger.debug("safe_reply_to: %s", exc)
    except Exception as exc:
        logger.debug("safe_reply_to unexpected: %s", exc)
    return None


def safe_send_message(chat_id: int, text: str, **kwargs):
    for attempt in range(3):
        try:
            return bot.send_message(chat_id, text, **kwargs)
        except ApiTelegramException as exc:
            if exc.error_code == 429:
                time.sleep(_parse_retry_after(str(exc)))
            elif exc.error_code in (502, 503, 504):
                time.sleep(3 * (attempt + 1))
            else:
                logger.debug("safe_send_message non-retryable [%s]: %s", exc.error_code, exc)
                return None
        except Exception as exc:
            logger.debug("safe_send_message exception: %s", exc)
            time.sleep(2)
    return None


def safe_answer_callback(call_id: str, text: str = None, show_alert: bool = False) -> None:
    if text and len(str(text)) > 200:
        text = str(text)[:197] + "..."
    try:
        bot.answer_callback_query(call_id, text, show_alert=show_alert)
    except ApiTelegramException as exc:
        if "query is too old" in str(exc) or "query ID is invalid" in str(exc):
            pass
        elif exc.error_code == 429:
            time.sleep(_parse_retry_after(str(exc)))
            try:
                bot.answer_callback_query(call_id, text, show_alert=show_alert)
            except Exception:
                pass
    except Exception:
        pass


def safe_edit_message(text: str, chat_id: int, msg_id: int, **kwargs):
    for _ in range(2):
        try:
            return bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", **kwargs)
        except ApiTelegramException as exc:
            if "message is not modified" in str(exc):
                return None
            if exc.error_code in (400, 404):
                return None
            if exc.error_code == 429:
                time.sleep(_parse_retry_after(str(exc)))
                continue
            logger.debug("safe_edit_message: %s", exc)
            return None
        except Exception as exc:
            logger.debug("safe_edit_message unexpected: %s", exc)
            return None
    return None

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ACCESS CONTROL
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def is_joined(uid: int) -> bool:
    if uid in OWNER_IDS or not REQUIRED_CHANNELS:
        return True
    with _join_cache_lock:
        cached = _join_cache.get(uid)
        if cached and (time.time() - cached[1]) < _JOIN_CACHE_TTL:
            return cached[0]
    for ch in REQUIRED_CHANNELS:
        if not ch or ch.startswith("-"):
            continue
        try:
            m = bot.get_chat_member(ch, uid)
            if m.status in ("left", "kicked"):
                with _join_cache_lock:
                    _join_cache[uid] = (False, time.time())
                return False
        except ApiTelegramException as exc:
            logger.warning("is_joined failed ch=%s uid=%s: %s", ch, uid, exc)
            with _join_cache_lock:
                _join_cache[uid] = (False, time.time())
            return False
        except Exception as exc:
            logger.warning("is_joined unexpected ch=%s uid=%s: %s", ch, uid, exc)
            with _join_cache_lock:
                _join_cache[uid] = (False, time.time())
            return False
    with _join_cache_lock:
        _join_cache[uid] = (True, time.time())
    return True


def check_access(uid: int) -> str:
    if uid in OWNER_IDS:
        return "OK"
    if is_banned(uid):
        return "BANNED"
    if not is_joined(uid):
        return "JOIN_REQ"
    return "OK"


def gate(message) -> bool:
    if not message.from_user:
        return False
    uid = message.from_user.id
    if _shutdown_event.is_set() and uid not in OWNER_IDS:
        safe_reply_to(
            message,
            f"🔴  <b>sʏsᴛᴇᴍ ᴏғғʟɪɴᴇ</b>\n<i>{DIV}</i>\n"
            "ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ ᴄʜᴀʟ ʀᴀʜɪ ʜᴀɪ.\n📢  @NeuroLacera",
        )
        return False
    status = check_access(uid)
    if status == "BANNED":
        safe_reply_to(
            message,
            "🚫  <b>ᴀᴄᴄᴇss ʀᴇᴠᴏᴋᴇᴅ</b>\n<i>ᴀᴀᴘᴋᴀ ᴀᴄᴄᴏᴜɴᴛ sᴜsᴘᴇɴᴅ ᴋɪʏᴀ ɢᴀʏᴀ ʜᴀɪ.</i>",
        )
        return False
    if status == "JOIN_REQ":
        safe_send_message(
            message.chat.id,
            f"🔒  <b>ᴀᴄᴄᴇss ʀᴇsᴛʀɪᴄᴛᴇᴅ</b>\n<i>{DIV}</i>\n"
            "ᴄʜᴀɴɴᴇʟs ᴊᴏɪɴ ᴋᴀʀᴏ ᴘʜɪʀ ᴀᴀɴᴀ.",
            reply_markup=mk_join(),
        )
        return False
    return True


def alert_admins(err: str, cmd: str, uid: int, username) -> None:
    for aid in OWNER_IDS:
        try:
            bot.send_message(
                aid,
                "🔴  <b>ᴇʀʀᴏʀ ᴀʟᴇʀᴛ</b>\n"
                f"<i>{DIV}</i>\n"
                f"⌨️  ᴄᴍᴅ   ›  <code>/{html.escape(str(cmd))}</code>\n"
                f"👤  ᴜsᴇʀ  ›  @{html.escape(str(username or 'N/A'))} "
                f"<code>({uid})</code>\n"
                f"💬  ᴇʀʀ   ›  <code>{html.escape(str(err)[:300])}</code>",
                parse_mode="HTML",
            )
        except Exception:
            pass

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  MARKUP BUILDERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def mk_join() -> InlineKeyboardMarkup:
    mu = InlineKeyboardMarkup()
    for ch in REQUIRED_CHANNELS:
        if ch.startswith("-"):
            continue
        ch_handle = ch.lstrip("@")
        mu.add(InlineKeyboardButton(
            f"📢  ᴊᴏɪɴ @{ch_handle}",
            url=f"https://t.me/{ch_handle}",
        ))
    if REQUIRED_CHANNELS:
        mu.add(InlineKeyboardButton("✅  ᴊᴏɪɴ ᴋᴀʀ ʟɪʏᴀ  —  ᴠᴇʀɪғʏ", callback_data="check_join"))
    return mu


def mk_buy() -> InlineKeyboardMarkup:
    mu = InlineKeyboardMarkup(row_width=2)
    mu.add(
        InlineKeyboardButton("💳  ʙᴜʏ ᴄʀᴇᴅɪᴛs", url=f"https://t.me/{ADMIN_USERNAME}"),
        InlineKeyboardButton("🔗  ʀᴇғᴇʀ & ᴇᴀʀɴ", callback_data="refer_now"),
    )
    mu.add(
        InlineKeyboardButton("📢  ᴜᴘᴅᴀᴛᴇs", url="https://t.me/NeuroLacera"),
        InlineKeyboardButton("🤖  ʙᴏᴛ", url="https://t.me/LaceraOsintBot"),
    )
    return mu


def mk_search_done(qid: str, cur_p: int, total: int) -> InlineKeyboardMarkup:
    mu = InlineKeyboardMarkup(row_width=3)
    if total > 1:
        prev_p = (cur_p - 1) % total
        next_p = (cur_p + 1) % total
        mu.add(
            InlineKeyboardButton("‹", callback_data=f"pg_{qid}_{prev_p}"),
            InlineKeyboardButton(f"◈  {cur_p + 1} / {total}", callback_data="none"),
            InlineKeyboardButton("›", callback_data=f"pg_{qid}_{next_p}"),
        )
    mu.add(
        InlineKeyboardButton("📋  ᴄᴏᴘʏ",        callback_data=f"copy_{qid}_{cur_p}"),
        InlineKeyboardButton("🔍  ɴᴇᴡ sᴇᴀʀᴄʜ", callback_data="new_search"),
        InlineKeyboardButton("◉  ᴘʀᴏғɪʟᴇ",      callback_data="my_profile"),
    )
    return mu

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  USER COMMANDS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.message_handler(commands=["start"])
def cmd_start(message):
    if not message.from_user:
        return
    uid = message.from_user.id
    if is_banned(uid) and uid not in OWNER_IDS:
        return

    username   = message.from_user.username  or None
    first_name = message.from_user.first_name or None

    args = (message.text or "").split()
    if len(args) > 1 and args[1].isdigit():
        ref_id = int(args[1])
        if ref_id > 0 and ref_id != uid and add_referral(uid, ref_id):
            try:
                safe_send_message(
                    ref_id,
                    f"🎁  <b>ʀᴇғᴇʀʀᴀʟ ʙᴏɴᴜs</b>\n<i>{DIV}</i>\n"
                    "ɴᴀʏᴀ ᴜsᴇʀ ᴊᴏɪɴ ʜᴏ ɢᴀʏᴀ.\n"
                    "✦  <b>+2 ᴄʀᴇᴅɪᴛs</b> ᴀᴅᴅ ʜᴏ ɢᴀʏᴇ.",
                )
            except Exception:
                pass

    get_user(uid)
    update_user_info(uid, username, first_name)

    name = html.escape(message.from_user.first_name or "Operative")
    safe_send_message(
        message.chat.id,
        "┌─────────────────────────┐\n"
        "  ⚡  <b>ʟᴀᴄᴇʀᴀ ᴏsɪɴᴛ</b>  ·  ᴅᴇᴇᴘ ɪɴᴛᴇʟ\n"
        "└─────────────────────────┘\n\n"
        f"ᴡᴇʟᴄᴏᴍᴇ, <b>{name}</b> 👾\n"
        "ᴍᴜʟᴛɪ-ʟᴀʏᴇʀ ᴅᴀᴛᴀ ɪɴᴛᴇʟ ᴀᴛ ʏᴏᴜʀ ᴄᴏᴍᴍᴀɴᴅ.\n\n"
        "◈  <b>sᴇᴀʀᴄʜ ᴍᴏᴅᴇs</b>\n"
        f"<i>{'·' * 26}</i>\n"
        "  📞  /number   <code>+91 / +92 / +880</code>\n"
        "  📧  /email    <code>user@mail.com</code>\n"
        "  🪪  /aadhar   <code>XXXXXXXXXXXX</code>\n"
        "  💳  /pan      <code>ABCDE1234F</code>\n"
        "  🚗  /vehicle  <code>MH12AB1234</code>\n"
        "  🌐  /ip       <code>1.2.3.4</code>\n\n"
        "◈  <b>ᴀᴄᴄᴏᴜɴᴛ</b>\n"
        f"<i>{'·' * 26}</i>\n"
        "  📊  /profile  ·  🔗  /refer  ·  🎫  /redeem\n\n"
        f"<i>⚡  <a href='https://t.me/NeuroLacera'>@NeuroLacera</a>  ·  "
        f"<a href='https://t.me/LaceraOsintBot'>@LaceraOsintBot</a></i>",
    )


@bot.message_handler(commands=["help"])
def cmd_help(message):
    if not gate(message):
        return
    uid    = message.from_user.id
    is_own = uid in OWNER_IDS

    text = (
        "📖  <b>ᴄᴏᴍᴍᴀɴᴅ ʀᴇғᴇʀᴇɴᴄᴇ</b>\n"
        f"<i>{DIV}</i>\n\n"
        "◈  <b>sᴇᴀʀᴄʜ</b>\n"
        f"<i>{DIV}</i>\n"
        "  📞  <b>/number</b>  <code>+91/+92/+880 ...</code>\n"
        "  📧  <b>/email</b>   <code>user@domain.com</code>\n"
        "  🪪  <b>/aadhar</b>  <code>XXXX XXXX XXXX</code>\n"
        "  💳  <b>/pan</b>     <code>ABCDE1234F</code>\n"
        "  🚗  <b>/vehicle</b> <code>MH12AB1234</code>\n"
        "  🌐  <b>/ip</b>      <code>1.2.3.4</code>\n\n"
        "◈  <b>ᴀᴄᴄᴏᴜɴᴛ</b>\n"
        f"<i>{DIV}</i>\n"
        "  📊  /profile  —  sᴛᴀᴛs & ᴄʀᴇᴅɪᴛs\n"
        "  🔗  /refer    —  +2 ᴄʀᴇᴅɪᴛs ᴘʀᴏ ɪɴᴠɪᴛᴇ\n"
        "  🎫  /redeem   —  ᴘʀᴏᴍᴏ ᴄᴏᴅᴇ\n\n"
        "◈  <b>ʀᴜʟᴇs</b>\n"
        f"  ·  {DAILY_LIMIT} ғʀᴇᴇ sᴇᴀʀᴄʜ/ᴅᴀʏ\n"
        "  ·  ᴜsᴋᴇ ʙᴀᴀᴅ 1 ᴄʀᴇᴅɪᴛ / sᴇᴀʀᴄʜ\n"
        f"  ·  ʀᴇsᴜʟᴛ ᴀᴜᴛᴏ-ᴅᴇʟᴇᴛᴇ {AUTO_DELETE_SECS}s ᴍᴇ\n"
        "  ·  ɴᴜᴍʙᴇʀ: 🇮🇳 🇵🇰 🇧🇩 ᴏɴʟʏ"
    )
    if is_own:
        text += (
            f"\n\n<i>{DIV}</i>\n"
            "👑  <b>ᴀᴅᴍɪɴ</b>\n"
            "  /ownerbot  /stats  /ping  /broadcast\n"
            "  /makecode  /usedcode  /giveall  /addcredits\n"
            "  /userlist  /userinfo  /detail\n"
            "  /ban  /unban  /lock  /unlock  /listlocked  /shutdown"
        )
    safe_send_message(message.chat.id, text)


@bot.message_handler(commands=["profile"])
def cmd_profile(message):
    if not gate(message):
        return
    uid = message.from_user.id
    u   = get_user(uid)
    if not u:
        return safe_reply_to(message, "❌  ᴘʀᴏғɪʟᴇ ʟᴏᴀᴅ ɴᴀʜɪ ʜᴜᴀ.")
    credits, daily_used, daily_reset, _, refer_count = u[1], u[2], u[3], u[4], u[5]
    remaining = max(0, DAILY_LIMIT - daily_used)
    if credits >= 100:  tier = "💎 ᴘʀᴇᴍɪᴜᴍ"
    elif credits >= 20: tier = "⭐ sɪʟᴠᴇʀ"
    else:               tier = "🆓 ʙᴀsɪᴄ"
    safe_send_message(
        message.chat.id,
        "┌─────────────────────────┐\n"
        "  ◉  <b>ᴏᴘᴇʀᴀᴛɪᴠᴇ ᴘʀᴏғɪʟᴇ</b>\n"
        "└─────────────────────────┘\n"
        f"🆔  ɪᴅ         <code>{uid}</code>\n"
        f"📡  sᴛᴀᴛᴜs    🟢 ᴀᴄᴛɪᴠᴇ\n"
        f"🏷️  ᴛɪᴇʀ      {tier}\n"
        f"💎  ᴄʀᴇᴅɪᴛs   <code>{credits}</code>\n\n"
        "◈  <b>ᴅᴀɪʟʏ</b>\n"
        f"  {fmt_bar(daily_used, DAILY_LIMIT)}\n"
        f"  🆓  ʙᴀᴄʜᴇ    ›  <code>{remaining} / {DAILY_LIMIT}</code>\n"
        f"  ⏱️  ʀᴇsᴇᴛ    ›  <code>{fmt_reset(daily_reset)}</code>\n\n"
        f"<i>{DIV}</i>\n"
        f"🔗  ʀᴇғᴇʀʀᴀʟs   <code>{refer_count}</code>\n"
        "<i>ᴜsᴇ /refer ᴛᴏ ᴇᴀʀɴ ᴍᴏʀᴇ</i>",
        reply_markup=mk_buy(),
    )


@bot.message_handler(commands=["refer"])
def cmd_refer(message):
    if not gate(message):
        return
    uid   = message.from_user.id
    u     = get_user(uid)
    count = u[5] if u else 0
    link  = f"https://t.me/{_BOT_USERNAME}?start={uid}"
    share_url = f"https://t.me/share/url?url={link}&text=Join%20LaceraOSINT%20for%20free%20intelligence%20lookups!"
    mu_ref = InlineKeyboardMarkup()
    mu_ref.add(InlineKeyboardButton("📤  sʜᴀʀᴇ ʟɪɴᴋ", url=share_url))
    safe_send_message(
        message.chat.id,
        f"🔗  <b>ʀᴇғᴇʀʀᴀʟ ᴘʀᴏɢʀᴀᴍ</b>\n<i>{DIV}</i>\n"
        "ɪɴᴠɪᴛᴇ ᴋᴀʀᴏ, ᴄʀᴇᴅɪᴛs ᴋᴀᴍᴀᴏ.\n\n"
        "🎁  ʀᴇᴡᴀʀᴅ  ›  <b>+2 ᴄʀᴇᴅɪᴛs</b> ᴘʀᴏ ɴʏᴜ ʏᴜᴢᴀʀ\n\n"
        f"🔗  <b>ᴛᴇʀᴀ ʟɪɴᴋ</b>\n<code>{link}</code>\n\n"
        f"<i>{DIV}</i>\n"
        f"👥  ᴛᴏᴛᴀʟ ʀᴇғᴇʀʀᴀʟs  ›  <code>{count}</code>",
        reply_markup=mu_ref,
    )


@bot.message_handler(commands=["redeem"])
def cmd_redeem(message):
    if not gate(message):
        return
    args = (message.text or "").split()
    if len(args) < 2:
        return safe_reply_to(message, "🎫  <b>ᴜsᴀɢᴇ</b>\n<code>/redeem YOUR-CODE</code>")
    raw_code = args[1].strip().upper()[:32]
    if not re.match(r"^[A-Z0-9\-]+$", raw_code):
        return safe_reply_to(message, "❌  <b>ɪɴᴠᴀʟɪᴅ ᴄᴏᴅᴇ ғᴏʀᴍᴀᴛ.</b>")
    result = redeem_code(message.from_user.id, raw_code)
    if result is None:
        safe_reply_to(message, "❌  <b>ɢᴀʟᴀᴛ ᴄᴏᴅᴇ ʜᴀɪ.</b>")
    elif result == -1:
        safe_reply_to(message, "⚠️  <b>ʏᴇ ᴄᴏᴅᴇ ᴘᴇʜʟᴇ ʜɪ ʟᴀɢᴀ ᴄʜᴜᴋᴀ ʜᴀɪ.</b>")
    elif result == -2:
        safe_reply_to(message, "⏳  <b>ᴄᴏᴅᴇ ᴇxᴘᴀɪʀ ʜᴏ ɢᴀʏᴀ.</b>")
    else:
        safe_reply_to(
            message,
            f"✅  <b>ʀᴇᴅᴇᴇᴍ sᴜᴄᴄᴇss</b>\n<i>{DIV}</i>\n"
            f"💎  <b>+{result} ᴄʀᴇᴅɪᴛs</b> ᴀᴅᴅ ʜᴏ ɢᴀʏᴇ.",
        )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SEARCH SYSTEM
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.message_handler(commands=["number", "email", "vehicle", "aadhar", "pan", "ip"])
def cmd_lookup(message):
    if not gate(message):
        return
    if not message.text:
        return
    raw_cmd = message.text.split()[0][1:]
    cmd     = raw_cmd.split("@")[0].lower()
    args    = message.text.split(maxsplit=1)
    MODE_EMOJI = {
        "number": "📞", "email": "📧", "aadhar": "🪪",
        "pan": "💳", "vehicle": "🚗", "ip": "🌐",
    }
    emoji = MODE_EMOJI.get(cmd, "🔍")
    if len(args) < 2:
        # Prompt for input
        if cmd == "number":
            prompt = (
                f"{emoji}  <b>ɴᴜᴍʙᴇʀ ᴅᴀᴀʟᴏ</b>\n"
                f"<i>{DIV}</i>\n"
                "🇮🇳  <code>+91XXXXXXXXXX</code>  ʏᴀ  <code>XXXXXXXXXX</code>\n"
                "🇵🇰  <code>+92XXXXXXXXXX</code>\n"
                "🇧🇩  <code>+880XXXXXXXXXX</code>"
            )
        else:
            prompt = f"{emoji}  <b>{cmd.upper()} ᴅᴀᴀʟᴏ</b>"
        sent = safe_reply_to(message, prompt)
        if sent:
            bot.register_next_step_handler(sent, lambda m, c=cmd: do_search(m, c))
    else:
        do_search(message, cmd, args[1].strip())


def do_search(message, mode: str, query: str = None) -> None:
    if not message.from_user:
        return
    uid = message.from_user.id

    if query is None:
        try:
            msg_age = time.time() - (message.date or 0)
        except Exception:
            msg_age = 0
        if msg_age > 300:
            safe_reply_to(message, "⏱️  <b>sᴇssɪᴏɴ ᴛɪᴍᴇᴅ ᴏᴜᴛ.</b>  ᴅᴏʙᴀʀᴀ ᴄᴏᴍᴍᴀɴᴅ ᴜsᴇ ᴋᴀʀᴏ.")
            return

    if _shutdown_event.is_set() and uid not in OWNER_IDS:
        safe_reply_to(message, "🔴  <b>sʏsᴛᴇᴍ ᴏғғʟɪɴᴇ</b>  —  ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ.")
        return

    access = check_access(uid)
    if access == "BANNED":
        safe_reply_to(message, "🚫  <b>ᴀᴄᴄᴇss ʀᴇᴠᴏᴋᴇᴅ.</b>")
        return
    if access == "JOIN_REQ":
        safe_send_message(
            message.chat.id,
            f"🔒  <b>ᴀᴄᴄᴇss ʀᴇsᴛʀɪᴄᴛᴇᴅ</b>\n<i>{DIV}</i>\n"
            "ᴄʜᴀɴɴᴇʟs ᴊᴏɪɴ ᴋᴀʀᴏ ᴘʜɪʀ ᴀᴀɴᴀ.",
            reply_markup=mk_join(),
        )
        return

    try:
        update_user_info(uid, message.from_user.username or None, message.from_user.first_name or None)
    except Exception:
        pass

    raw_q = (query or message.text or "").strip()
    if raw_q.startswith("/"):
        safe_reply_to(message, "↩️  <b>ᴄᴀɴᴄᴇʟ.</b>")
        bot.process_new_messages([message])
        return
    if not raw_q:
        safe_reply_to(message, "❌  ᴋᴜᴄʜ ᴛᴏ ᴅᴀᴀʟᴏ.")
        return

    # Cooldown
    if uid not in OWNER_IDS:
        now = time.time()
        with _cooldown_lock:
            last = USER_COOLDOWN.get(uid, 0)
            if now - last < COOLDOWN_SECONDS:
                left = int(COOLDOWN_SECONDS - (now - last)) + 1
                safe_reply_to(message, f"⏳  <b>ᴄᴏᴏʟᴅᴏᴡɴ</b>  ›  <code>{left}s</code>")
                return
            USER_COOLDOWN[uid] = now

    # Validate + normalize
    if mode == "number":
        e164, cc = normalize_phone(raw_q)
        if not e164:
            safe_reply_to(
                message,
                "❌  <b>ɪɴᴠᴀʟɪᴅ ɴᴜᴍʙᴇʀ</b>\n\n"
                "◈  <b>ᴀʟʟᴏᴡᴇᴅ ᴄᴏᴜɴᴛʀɪᴇs:</b>\n"
                "  🇮🇳  <code>+91XXXXXXXXXX</code>  (10 ᴀɴᴋ ᴋᴇ ʙᴀᴀᴅ)\n"
                "  🇮🇳  <code>XXXXXXXXXX</code>  (bare 10 ᴅɪɢɪᴛ = ɪɴᴅɪᴀ)\n"
                "  🇵🇰  <code>+92XXXXXXXXXX</code>\n"
                "  🇧🇩  <code>+880XXXXXXXXXX</code>\n\n"
                "<i>⚠️ ᴅᴏ sʜᴜʀᴜ sᴇ ʟɪᴋʜᴏ, ʙɪᴄʜ ᴍᴇ ɢᴀᴘ ɴᴀʜɪ</i>",
            )
            return
        q = e164
        flag, country_name = get_allowed_cc_info(cc)
    else:
        q, verr = validate_query(raw_q, mode)
        if verr:
            safe_reply_to(message, verr)
            return
        flag, country_name = detect_country(q, mode)

    # Credit check (stale read)
    user = get_user(uid)
    if not user:
        safe_reply_to(message, "❌  ᴜsᴇʀ ᴅᴀᴛᴀ ʟᴏᴀᴅ ɴᴀʜɪ ʜᴜᴀ.")
        return
    credits, daily_used = user[1], user[2]
    if daily_used >= DAILY_LIMIT and credits <= 0 and uid not in OWNER_IDS:
        safe_reply_to(
            message,
            f"⚠️  <b>ʟɪᴍɪᴛ ᴋʜᴀᴛᴀᴍ</b>\n<i>{DIV}</i>\n"
            f"ᴅᴀɪʟʏ: <code>{DAILY_LIMIT}</code>  ·  ᴄʀᴇᴅɪᴛs: <code>0</code>\n\n"
            "ʙᴜʏ ᴋᴀʀᴏ ʏᴀ ʀᴇғᴇʀ ᴋᴀʀᴏ.",
            reply_markup=mk_buy(),
        )
        return

    # Stealth lock
    if is_query_locked(q):
        def _stealth_deny(cid):
            time.sleep(random.uniform(2.5, 4.5))
            safe_send_message(cid, "🔍  <b>ᴋᴏɪ ʀᴇᴄᴏʀᴅ ɴᴀʜɪ ᴍɪʟᴀ.</b>")
        threading.Thread(target=_stealth_deny, args=(message.chat.id,), daemon=True).start()
        return

    country_line = f"{flag} <i>{html.escape(country_name)}</i>  ·  " if country_name != "Unknown" else ""

    # Searching indicator
    wait = None
    try:
        wait = bot.send_message(
            message.chat.id,
            "┌─────────────────────────┐\n"
            f"  ⟳  <b>ᴅʜᴜɴᴅʜ ʀᴀʜᴀ ʜᴜɴ...</b>\n"
            "└─────────────────────────┘\n"
            f"  {country_line}<code>{html.escape(q)}</code>\n"
            "  <i>ᴛʜᴏᴅᴀ ʀᴜᴋᴏ...</i>",
        )
    except Exception:
        pass

    try:
        results, _ = perform_lookup(q, mode)

        if wait:
            try:
                bot.delete_message(message.chat.id, wait.message_id)
            except Exception:
                pass

        if not results or not isinstance(results, list):
            safe_send_message(message.chat.id, "🔍  <b>ᴋᴏɪ ʀᴇᴄᴏʀᴅ ɴᴀʜɪ ᴍɪʟᴀ.</b>")
            return

        first   = results[0]
        _err_starts = ("🔍", "❌", "⚠️", "⏱️", "⏳", "🌐", "✦  <b>sᴇʀᴠɪᴄᴇ")
        _err_subs   = ("<b>ɴᴏ ʀᴇᴄᴏʀᴅs", "<b>ᴀᴘɪ", "<b>ɪɴᴠᴀʟɪᴅ", "<b>sᴇʀᴠᴇʀ", "<b>ɴᴇᴛᴡᴏʀᴋ")
        first_s = first.lstrip()
        is_real = (
            not any(first_s.startswith(p) for p in _err_starts)
            and not any(s in first for s in _err_subs)
        )

        if is_real and uid not in OWNER_IDS:
            if not deduct_credit_atomic(uid, DAILY_LIMIT):
                safe_reply_to(message, "⚠️  <b>ʟɪᴍɪᴛ ᴋʜᴀᴛᴀᴍ</b>", reply_markup=mk_buy())
                return
        if is_real:
            log_search(uid, q, mode, country_name)

        qid = "".join(random.choices(string.ascii_letters + string.digits, k=12))
        with _cash_lock:
            cash_reports[qid] = {"pages": results, "ts": time.time()}
        markup = mk_search_done(qid, 0, len(results))

        try:
            sent = bot.send_message(
                message.chat.id,
                results[0] + WATERMARK,
                reply_markup=markup,
                disable_web_page_preview=True,
            )
            _spawn_delete(message.chat.id, sent.message_id)
        except ApiTelegramException as exc:
            if exc.error_code == 400 and "message is too long" in str(exc):
                plain = re.sub(r"<[^>]+>", "", results[0])[:3500]
                safe_send_message(
                    message.chat.id,
                    f"<code>{html.escape(plain)}</code>\n\n⚡ @LaceraOsintBot",
                    reply_markup=markup,
                )
            else:
                plain = re.sub(r"<[^>]+>", "", results[0])
                safe_send_message(
                    message.chat.id,
                    plain + "\n\n⚡ @LaceraOsintBot",
                    reply_markup=markup,
                )

    except Exception as exc:
        if wait:
            try:
                bot.delete_message(message.chat.id, wait.message_id)
            except Exception:
                pass
        safe_send_message(
            message.chat.id,
            f"⚠️  <b>ᴋᴜᴄʜ ɢᴀᴅʙᴀᴅ ʜᴏ ɢᴀʏɪ</b>\n<i>{DIV}</i>\n"
            "ᴛʜᴏᴅɪ ᴅᴇʀ ʙᴀᴀᴅ ᴅᴏʙᴀʀᴀ ᴋᴀʀᴏ. 🙏",
        )
        alert_admins(str(exc), mode, uid, message.from_user.username)
        logger.error("do_search uid=%s mode=%s: %s", uid, mode, exc, exc_info=True)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ADMIN COMMANDS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _admin_guard(message) -> bool:
    return bool(message.from_user) and is_admin(message.from_user.id)


@bot.message_handler(commands=["ownerbot"])
def cmd_ownerbot(message):
    if not _admin_guard(message):
        return
    safe_reply_to(
        message,
        "┌─────────────────────────┐\n"
        "  👑  <b>ᴀᴅᴍɪɴ ᴄᴏɴᴛʀᴏʟ ᴘᴀɴᴇʟ</b>\n"
        "└─────────────────────────┘\n"
        "  📊  /stats           —  sʏsᴛᴇᴍ ᴏᴠᴇʀᴠɪᴇᴡ\n"
        "  🏓  /ping            —  ʟᴀᴛᴇɴᴄʏ & ᴜᴘᴛɪᴍᴇ\n"
        "  📢  /broadcast       —  ᴍᴀss ᴍᴇssᴀɢᴇ\n"
        f"<i>{DIV}</i>\n"
        "  🎫  /makecode [ᴀᴍᴛ] [ᴛɪᴍᴇ]\n"
        "  🔍  /usedcode [ᴄᴏᴅᴇ]\n"
        "  🎁  /giveall [ᴀᴍᴛ]\n"
        "  💰  /addcredits [ᴜɪᴅ] [ᴀᴍᴛ]\n"
        f"<i>{DIV}</i>\n"
        "  👥  /userlist  👤  /userinfo [ᴜɪᴅ]  🕵️  /detail [ᴜɪᴅ]\n"
        "  🚫  /ban [ᴜɪᴅ]  ✅  /unban [ᴜɪᴅ]\n"
        "  🔒  /lock [ǫ]   🔓  /unlock [ǫ]   📋  /listlocked\n"
        "  🛑  /shutdown [on/off]",
    )


@bot.message_handler(commands=["ping"])
def cmd_ping(message):
    if not _admin_guard(message):
        return
    t0  = time.time()
    msg = safe_reply_to(message, "⟳  ᴘɪɴɢɪɴɢ...")
    if not msg:
        return
    lat = round((time.time() - t0) * 1000, 1)
    safe_edit_message(
        f"🏓  <b>ᴘᴏɴɢ</b>\n<i>{DIV}</i>\n"
        f"⚡  ʟᴀᴛᴇɴᴄʏ  ›  <code>{lat} ms</code>\n"
        f"⏱️  ᴜᴘᴛɪᴍᴇ   ›  <code>{fmt_uptime(time.time() - BOT_START_TIME)}</code>\n"
        f"💾  ᴄᴀᴄʜᴇ    ›  <code>{len(cash_reports)} ᴇɴᴛʀɪᴇs</code>",
        message.chat.id, msg.message_id,
    )


@bot.message_handler(commands=["stats"])
def cmd_stats(message):
    if not _admin_guard(message):
        return
    users         = get_all_users_detail()
    total         = len(users)
    banned        = sum(1 for u in users if u[4] == 1)
    locked        = len(get_locked_list())
    today         = get_today_search_count()
    total_s       = get_total_search_count()
    mode_stats    = get_search_stats_by_mode()
    country_stats = get_search_stats_by_country()

    mode_lines = ""
    for mode, cnt in sorted(mode_stats.items(), key=lambda x: -x[1]):
        mode_lines += f"  ›  {mode.upper():<8}  <code>{cnt}</code>\n"

    country_lines = ""
    cc_flags = {"India": "🇮🇳", "Pakistan": "🇵🇰", "Bangladesh": "🇧🇩", "Unknown": "🌍"}
    for c_name, cnt in country_stats[:5]:
        c_name = str(c_name) if c_name else "Unknown"
        flag   = cc_flags.get(c_name, "🌍")
        country_lines += f"  ›  {flag} {html.escape(c_name):<14}  <code>{cnt}</code>\n"

    text = (
        "┌─────────────────────────┐\n"
        "  📊  <b>sʏsᴛᴇᴍ sᴛᴀᴛs</b>\n"
        "└─────────────────────────┘\n"
        f"👥  ᴛᴏᴛᴀʟ ᴜsᴇʀs       ›  <code>{total}</code>\n"
        f"🚫  ʙᴀɴɴᴇᴅ            ›  <code>{banned}</code>\n"
        f"🟢  ᴀᴄᴛɪᴠᴇ            ›  <code>{total - banned}</code>\n"
        f"🔒  ʟᴏᴄᴋᴇᴅ            ›  <code>{locked}</code>\n"
        f"<i>{DIV}</i>\n"
        f"🔍  ᴀᴀᴊ ᴋɪ sᴇᴀʀᴄʜ   ›  <code>{today}</code>\n"
        f"📈  ᴛᴏᴛᴀʟ sᴇᴀʀᴄʜ    ›  <code>{total_s}</code>\n"
        f"💾  ᴄᴀᴄʜᴇ            ›  <code>{len(cash_reports)}</code>\n"
        f"⏱️  ᴜᴘᴛɪᴍᴇ           ›  <code>{fmt_uptime(time.time() - BOT_START_TIME)}</code>"
    )
    if mode_lines:
        text += f"\n<i>{DIV}</i>\n🔎  <b>ᴍᴏᴅᴇ ʙʀᴇᴀᴋᴅᴏᴡɴ</b>\n" + mode_lines
    if country_lines:
        text += f"<i>{DIV}</i>\n🌍  <b>ᴛᴏᴘ ᴄᴏᴜɴᴛʀɪᴇs</b>\n" + country_lines
    if len(text) > 4000:
        text = text[:3990] + "\n<i>...</i>"
    safe_reply_to(message, text)


@bot.message_handler(commands=["broadcast"])
def cmd_broadcast(message):
    if not _admin_guard(message):
        return
    if not message.reply_to_message:
        return safe_reply_to(message, "📢  ᴋɪsɪ ᴍᴇssᴀɢᴇ ᴋᴏ ʀᴇᴘʟʏ ᴋᴀʀᴋᴇ /broadcast ᴅᴏ.")
    users = get_all_users()
    mu = InlineKeyboardMarkup()
    mu.add(
        InlineKeyboardButton(
            "✅  ʜᴀᴀɴ ʙʜᴇᴊᴏ",
            callback_data=f"bc_confirm_{message.chat.id}_{message.reply_to_message.message_id}",
        ),
        InlineKeyboardButton("❌  ᴄᴀɴᴄᴇʟ", callback_data="bc_cancel"),
    )
    safe_reply_to(
        message,
        f"📢  <b>ʙʀᴏᴀᴅᴄᴀsᴛ</b>\n<i>{DIV}</i>\n"
        f"👥  ᴛᴏᴛᴀʟ  ›  <code>{len(users)}</code>\n\n"
        "ᴄᴏɴғɪʀᴍ ᴋᴀʀᴏ?",
        reply_markup=mu,
    )


@bot.message_handler(commands=["makecode"])
def cmd_makecode(message):
    if not _admin_guard(message):
        return
    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        return safe_reply_to(
            message,
            "🎫  <b>ᴜsᴀɢᴇ:</b>  <code>/makecode [ᴀᴍᴛ] [ᴛɪᴍᴇ]</code>\n\n"
            "<b>ᴛɪᴍᴇ:</b>\n"
            "  <code>30s</code>  <code>10m</code>  <code>2h</code>  <code>1d</code>\n"
            "  <i>(no time = never expires)</i>",
        )
    amt = int(parts[1])
    if amt <= 0 or amt > 10000:
        return safe_reply_to(message, "❌  ᴀᴍᴛ 1–10000 ʜᴏɴᴀ ᴄʜᴀʜɪʏᴇ.")

    expiry = None
    expiry_text = "⏳  ᴇxᴘɪʀʏ   ›  <code>ɴᴇᴠᴇʀ</code>"
    if len(parts) >= 3:
        secs = parse_duration(parts[2])
        if secs is None:
            return safe_reply_to(
                message,
                "❌  <b>ɪɴᴠᴀʟɪᴅ ᴛɪᴍᴇ</b>\n"
                "ᴜsᴇ: <code>30s</code>, <code>10m</code>, <code>2h</code>, <code>1d</code>",
            )
        expiry = int(time.time()) + secs
        if secs < 60:       dur_str = f"{secs}s"
        elif secs < 3600:   dur_str = f"{secs // 60}m {secs % 60}s"
        elif secs < 86400:  dur_str = f"{secs // 3600}h {(secs % 3600) // 60}m"
        else:               dur_str = f"{secs // 86400}d {(secs % 86400) // 3600}h"
        expiry_text = f"⏳  ᴇxᴘɪʀʏ   ›  <code>{dur_str}</code>  ({fmt_ts(expiry)})"

    code = "NX-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=8))
    if create_code(code, amt, expiry):
        safe_reply_to(
            message,
            "┌─────────────────────────┐\n"
            "  🎫  <b>ᴄᴏᴅᴇ ɢᴇɴᴇʀᴀᴛᴇᴅ</b>\n"
            "└─────────────────────────┘\n"
            f"🔑  ᴄᴏᴅᴇ   ›  <code>{code}</code>\n"
            f"💎  ᴠᴀʟᴜᴇ  ›  <code>{amt} ᴄʀᴇᴅɪᴛs</code>\n"
            f"{expiry_text}",
        )
    else:
        safe_reply_to(message, "❌  ᴄᴏᴅᴇ ᴄʀᴇᴀᴛᴇ ɴᴀʜɪ ʜᴜᴀ.")


@bot.message_handler(commands=["usedcode"])
def cmd_usedcode(message):
    if not _admin_guard(message):
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        return safe_reply_to(message, "❌  <code>/usedcode [CODE]</code>")
    code_text = parts[1].strip().upper()[:64]
    info = get_code_info(code_text)
    if not info:
        return safe_reply_to(message, "❌  ᴄᴏᴅᴇ ɴᴀʜɪ ᴍɪʟᴀ.")
    used_status = "✅ ʏᴜᴢ ʜᴏ ɢᴀʏᴀ" if info["used"] else "🟢 ᴀᴠᴀɪʟᴀʙʟᴇ"
    user_part = "—"
    if info["used_by"]:
        uname = f"@{html.escape(info['username'])}" if info["username"] else "N/A"
        fname = html.escape(info["first_name"] or "N/A")
        user_part = f"<code>{info['used_by']}</code>  {uname}  <i>{fname}</i>"
    safe_reply_to(
        message,
        f"┌─────────────────────────┐\n  🔍  <b>ᴄᴏᴅᴇ ɪɴғᴏ</b>\n└─────────────────────────┘\n"
        f"🔑  ᴄᴏᴅᴇ       ›  <code>{html.escape(code_text)}</code>\n"
        f"💎  ᴠᴀʟᴜᴇ      ›  <code>{info['value']} ᴄʀᴇᴅɪᴛs</code>\n"
        f"📡  sᴛᴀᴛᴜs     ›  {used_status}\n"
        f"⏳  ᴇxᴘɪʀʏ     ›  <code>{fmt_expiry(info['expiry'])}</code>\n"
        f"🕐  ᴄʀᴇᴀᴛᴇᴅ   ›  <code>{fmt_ts(info['created_at'])}</code>\n"
        f"<i>{DIV}</i>\n"
        f"👤  ʏᴜᴢ ʙʏ    ›  {user_part}\n"
        f"🕑  ʏᴜᴢ ᴀᴛ    ›  <code>{fmt_ts(info['used_at']) if info['used_at'] else '—'}</code>",
    )


@bot.message_handler(commands=["giveall"])
def cmd_giveall(message):
    if not _admin_guard(message):
        return
    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        return safe_reply_to(message, "❌  /giveall [ᴀᴍᴛ]")
    amt = int(parts[1])
    if amt <= 0 or amt > 100000:
        return safe_reply_to(message, "❌  ᴀᴍᴛ 1–100000.")
    mu = InlineKeyboardMarkup()
    mu.add(
        InlineKeyboardButton(f"✅  +{amt} sᴀʙᴋᴏ", callback_data=f"giveall_confirm_{amt}"),
        InlineKeyboardButton("❌  ᴄᴀɴᴄᴇʟ", callback_data="admin_cancel"),
    )
    safe_reply_to(
        message,
        f"⚠️  <b>ᴄᴏɴғɪʀᴍ ɢɪᴠᴇᴀʟʟ</b>\n<i>{DIV}</i>\n"
        f"sᴀʙ ᴜsᴇʀ ᴋᴏ <code>+{amt} ᴄʀᴇᴅɪᴛs</code> ᴅᴇɴᴇ ʜᴀɪɴ?",
        reply_markup=mu,
    )


@bot.message_handler(commands=["addcredits"])
def cmd_addcredits(message):
    if not _admin_guard(message):
        return
    parts = (message.text or "").split()
    if len(parts) < 3 or not parts[1].isdigit() or not parts[2].isdigit():
        return safe_reply_to(message, "❌  /addcredits [ᴜɪᴅ] [ᴀᴍᴛ]")
    target = int(parts[1])
    amt    = int(parts[2])
    if target <= 0:
        return safe_reply_to(message, "❌  ɪɴᴠᴀʟɪᴅ ᴜɪᴅ.")
    if amt <= 0 or amt > 100000:
        return safe_reply_to(message, "❌  ᴀᴍᴛ 1–100000.")
    new_bal = add_credits_to_user(target, amt)
    if new_bal is not False and new_bal is not None:
        safe_reply_to(
            message,
            f"✅  <b>ᴄʀᴇᴅɪᴛs ᴀᴅᴅᴇᴅ</b>\n<i>{DIV}</i>\n"
            f"👤  ᴜsᴇʀ     ›  <code>{target}</code>\n"
            f"💰  ᴀᴅᴅᴇᴅ   ›  <code>+{amt}</code>\n"
            f"💎  ʙᴀʟᴀɴᴄᴇ ›  <code>{new_bal}</code>",
        )
    else:
        safe_reply_to(message, "❌  ᴇʀʀᴏʀ ʏᴀ ᴜsᴇʀ ɴᴀʜɪ ᴍɪʟᴀ.")


@bot.message_handler(commands=["userlist"])
def cmd_userlist(message):
    if not _admin_guard(message):
        return
    users = get_all_users_detail()
    if not users:
        return safe_reply_to(message, "📭  ᴋᴏɪ ᴜsᴇʀ ɴᴀʜɪ.")
    total  = len(users)
    banned = sum(1 for u in users if u[4] == 1)
    chunks = [users[i:i + 15] for i in range(0, total, 15)]

    for idx, chunk in enumerate(chunks):
        lines = []
        for u in chunk:
            row_uid, row_uname, row_fname, row_credits, row_banned, row_refs = u
            icon       = "🚫" if row_banned else "🟢"
            uname_part = ("@" + html.escape(str(row_uname))) if row_uname else "—"
            name_part  = html.escape(str(row_fname or "N/A"))
            lines.append(
                f"{icon}  <code>{row_uid}</code>  {uname_part}\n"
                f"     👤  {name_part}\n"
                f"     💎  <code>{row_credits}</code>  ·  <code>{row_refs} ʀᴇғs</code>"
            )
        header = ""
        if idx == 0:
            header = (
                "┌─────────────────────────┐\n"
                "  👥  <b>ᴜsᴇʀ ʟɪsᴛ</b>\n"
                "└─────────────────────────┘\n"
                f"ᴛᴏᴛᴀʟ  ›  <code>{total}</code>  🟢  <code>{total - banned}</code>  🚫  <code>{banned}</code>\n"
                f"<i>{DIV}</i>\n\n"
            )
        page_lbl = f"\n\n<i>ᴘᴀɢᴇ {idx + 1} / {len(chunks)}</i>" if len(chunks) > 1 else ""
        full_msg = header + "\n\n".join(lines) + page_lbl
        if len(full_msg) > 4000:
            full_msg = full_msg[:3990] + "\n<i>...</i>"
        try:
            safe_send_message(message.chat.id, full_msg)
            if idx < len(chunks) - 1:
                time.sleep(0.5)
        except Exception as exc:
            logger.error("cmd_userlist chunk %d: %s", idx, exc)


@bot.message_handler(commands=["userinfo"])
def cmd_userinfo(message):
    if not _admin_guard(message):
        return
    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        return safe_reply_to(message, "❌  /userinfo [ᴜɪᴅ]")
    target = int(parts[1])
    if target <= 0:
        return safe_reply_to(message, "❌  ɪɴᴠᴀʟɪᴅ ᴜɪᴅ.")
    u = get_user(target)
    if not u:
        return safe_reply_to(message, "❌  ᴜsᴇʀ ɴᴀʜɪ ᴍɪʟᴀ.")
    uid, credits, daily_used, daily_reset, banned, refer_count = u
    safe_reply_to(
        message,
        "┌─────────────────────────┐\n"
        "  👤  <b>ᴜsᴇʀ ɪɴғᴏ</b>\n"
        "└─────────────────────────┘\n"
        f"🆔  ɪᴅ          ›  <code>{uid}</code>\n"
        f"📡  sᴛᴀᴛᴜs     ›  {'🚫  ʙᴀɴɴᴇᴅ' if banned else '🟢  ᴀᴄᴛɪᴠᴇ'}\n"
        f"💎  ᴄʀᴇᴅɪᴛs    ›  <code>{credits}</code>\n"
        f"📊  ᴅᴀɪʟʏ       ›  {fmt_bar(daily_used, DAILY_LIMIT)}\n"
        f"⏱️  ʀᴇsᴇᴛ ɪɴ   ›  <code>{fmt_reset(daily_reset)}</code>\n"
        f"🔗  ʀᴇғᴇʀʀᴀʟs  ›  <code>{refer_count}</code>",
    )


@bot.message_handler(commands=["detail"])
def cmd_detail(message):
    if not _admin_guard(message):
        return
    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        return safe_reply_to(message, "❌  /detail [ᴜɪᴅ]")
    target = int(parts[1])
    if target <= 0:
        return safe_reply_to(message, "❌  ɪɴᴠᴀʟɪᴅ ᴜɪᴅ.")
    logs = get_user_history(target, limit=30)
    if not logs:
        return safe_reply_to(message, f"📭  ᴋᴏɪ ʜɪsᴛʀʏ ɴᴀʜɪ  <code>{target}</code>.")
    text = f"🕵️  <b>sᴇᴀʀᴄʜ ʜɪsᴛʀʏ</b>  ›  <code>{target}</code>\n<i>{DIV}</i>\n\n"
    for i, (query, mode, ts) in enumerate(logs, 1):
        if ts is None:
            ts_str = "?"
        elif hasattr(ts, "strftime"):
            ts_str = ts.strftime("%d/%m/%y %H:%M")
        else:
            try:
                ts_str = datetime.datetime.fromtimestamp(int(ts)).strftime("%d/%m/%y %H:%M")
            except Exception:
                ts_str = str(ts)[:16]
        q_disp = html.escape(str(query)[:50])
        text += f"<code>{i:02d}</code>  {html.escape(str(mode).upper()):<8}  <code>{q_disp}</code>  <i>{ts_str}</i>\n"
    text += f"\n<i>ᴛᴏᴛᴀʟ  ›  {len(logs)}</i>"
    if len(text) > 4000:
        text = text[:3990] + "\n<i>...</i>"
    safe_reply_to(message, text)


@bot.message_handler(commands=["ban"])
def cmd_ban(message):
    if not _admin_guard(message):
        return
    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        return safe_reply_to(message, "❌  /ban [ᴜɪᴅ]")
    target = parts[1]
    mu = InlineKeyboardMarkup()
    mu.add(
        InlineKeyboardButton("🚫  ʜᴀᴀɴ ʙᴀɴ ᴋᴀʀᴏ", callback_data=f"ban_confirm_{target}"),
        InlineKeyboardButton("❌  ᴄᴀɴᴄᴇʟ", callback_data="admin_cancel"),
    )
    safe_reply_to(
        message,
        f"⚠️  ᴜsᴇʀ <code>{target}</code> ᴋᴏ ʙᴀɴ ᴋᴀʀɴᴀ ʜᴀɪ?",
        reply_markup=mu,
    )


@bot.message_handler(commands=["unban"])
def cmd_unban(message):
    if not _admin_guard(message):
        return
    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        return safe_reply_to(message, "❌  /unban [ᴜɪᴅ]")
    unban_user(int(parts[1]))
    safe_reply_to(message, f"✅  <code>{parts[1]}</code>  ᴜɴʙᴀɴ ʜᴏ ɢᴀʏᴀ.")


@bot.message_handler(commands=["lock"])
def cmd_lock(message):
    if not _admin_guard(message):
        return
    args = (message.text or "").split(maxsplit=1)
    if len(args) < 2 or not args[1].strip():
        return safe_reply_to(message, "❌  /lock [ǫᴜᴇʀʏ]")
    raw = args[1].strip()
    e164, _ = normalize_phone(raw)
    q = e164 if e164 else raw
    add_lock(q)
    safe_reply_to(message, f"🔒  ʟᴏᴄᴋᴇᴅ  ›  <code>{html.escape(q)}</code>")


@bot.message_handler(commands=["unlock"])
def cmd_unlock(message):
    if not _admin_guard(message):
        return
    args = (message.text or "").split(maxsplit=1)
    if len(args) < 2 or not args[1].strip():
        return safe_reply_to(message, "❌  /unlock [ǫᴜᴇʀʏ]")
    raw = args[1].strip()
    e164, _ = normalize_phone(raw)
    q = e164 if e164 else raw
    if remove_lock(q):
        safe_reply_to(message, f"🔓  ᴜɴʟᴏᴄᴋᴇᴅ  ›  <code>{html.escape(q)}</code>")
    else:
        safe_reply_to(message, "⚠️  ʟᴏᴄᴋ ʟɪsᴛ ᴍᴇ ɴᴀʜɪ ᴍɪʟᴀ.")


@bot.message_handler(commands=["listlocked"])
def cmd_listlocked(message):
    if not _admin_guard(message):
        return
    locked = get_locked_list()
    if not locked:
        return safe_reply_to(message, "📭  ᴋᴏɪ ʟᴏᴄᴋᴇᴅ ǫᴜᴇʀʏ ɴᴀʜɪ.")
    text = f"🔒  <b>ʟᴏᴄᴋᴇᴅ  ›  {len(locked)}</b>\n<i>{DIV}</i>\n\n"
    for i, q in enumerate(locked, 1):
        text += f"<code>{i:02d}</code>  <code>{html.escape(q)}</code>\n"
        if len(text) > 3500:
            text += "\n<i>...ᴀɴᴅ ᴍᴏʀᴇ</i>"
            break
    safe_reply_to(message, text)


@bot.message_handler(commands=["shutdown"])
def cmd_shutdown(message):
    if not _admin_guard(message):
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        return safe_reply_to(message, "❌  /shutdown [on/off]")
    if parts[1].lower() == "on":
        _shutdown_event.set()
        safe_reply_to(message, "🛑  <b>sʜᴜᴛᴅᴏᴡɴ ᴏɴ.</b>  ᴜsᴇʀs ᴋᴀ ᴀᴄᴄᴇss ʙɴᴅ.")
    elif parts[1].lower() == "off":
        _shutdown_event.clear()
        safe_reply_to(message, "✅  <b>sʏsᴛᴇᴍ ᴏɴʟɪɴᴇ.</b>")
    else:
        safe_reply_to(message, "❌  /shutdown on  ʏᴀ  /shutdown off")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CALLBACKS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _valid_qid(qid: str) -> bool:
    return bool(qid) and len(qid) <= 16 and qid.replace("-", "").isalnum()


@bot.callback_query_handler(func=lambda call: True)
def handle_cb(call):
    if not call.from_user:
        return safe_answer_callback(call.id)
    uid = call.from_user.id

    # ── Join verify ──
    if call.data == "check_join":
        with _join_cache_lock:
            _join_cache.pop(uid, None)
        if is_joined(uid):
            safe_answer_callback(call.id, "✅  ᴀᴄᴄᴇss ᴅᴇ ᴅɪʏᴀ!")
            safe_edit_message(
                f"✅  <b>ᴀᴄᴄᴇss ɢʀᴀɴᴛᴇᴅ!</b>\n<i>{DIV}</i>\n/start ᴋᴀʀᴏ.",
                call.message.chat.id, call.message.message_id,
            )
        else:
            safe_answer_callback(call.id, "❌  ᴘᴇʜʟᴇ sᴀʙ ᴄʜᴀɴɴᴇʟ ᴊᴏɪɴ ᴋᴀʀᴏ!", show_alert=True)

    # ── Refer ──
    elif call.data == "refer_now":
        u     = get_user(uid)
        count = u[5] if u else 0
        link  = f"https://t.me/{_BOT_USERNAME}?start={uid}"
        safe_send_message(
            call.message.chat.id,
            f"🔗  <b>ᴛᴇʀᴀ ʀᴇғᴇʀʀᴀʟ ʟɪɴᴋ</b>\n<i>{DIV}</i>\n<code>{link}</code>\n\n"
            f"👥  ᴛᴏᴛᴀʟ  ›  <code>{count}</code>  ·  🎁  <b>+2 ᴄʀᴇᴅɪᴛs</b> ᴘʀᴏ ɪɴᴠɪᴛᴇ",
        )
        safe_answer_callback(call.id)

    # ── New search ──
    elif call.data == "new_search":
        safe_answer_callback(call.id)
        safe_send_message(
            call.message.chat.id,
            f"🔍  <b>ɴʏᴀ sᴇᴀʀᴄʜ</b>\n<i>{DIV}</i>\n"
            "📞 /number  📧 /email  🪪 /aadhar\n"
            "💳 /pan  🚗 /vehicle  🌐 /ip",
        )

    # ── Profile inline ──
    elif call.data == "my_profile":
        safe_answer_callback(call.id)
        u = get_user(uid)
        if u:
            credits, daily_used, daily_reset, _, refer_count = u[1], u[2], u[3], u[4], u[5]
            remaining = max(0, DAILY_LIMIT - daily_used)
            safe_send_message(
                call.message.chat.id,
                f"◉  <b>ᴘʀᴏғɪʟᴇ</b>\n<i>{DIV}</i>\n"
                f"💎  ᴄʀᴇᴅɪᴛs   ›  <code>{credits}</code>\n"
                f"📊  {fmt_bar(daily_used, DAILY_LIMIT)}\n"
                f"🆓  ʙᴀᴄʜᴇ     ›  <code>{remaining}</code>\n"
                f"⏱️  ʀᴇsᴇᴛ     ›  <code>{fmt_reset(daily_reset)}</code>\n"
                f"🔗  ʀᴇғᴇʀʀᴀʟs ›  <code>{refer_count}</code>",
                reply_markup=mk_buy(),
            )

    # ── Pagination ──
    elif call.data.startswith("pg_"):
        parts = call.data.split("_", 2)
        if len(parts) != 3:
            return safe_answer_callback(call.id)
        _, qid, p_str = parts
        if not _valid_qid(qid):
            return safe_answer_callback(call.id)
        with _cash_lock:
            entry = cash_reports.get(qid)
        if entry is None:
            return safe_answer_callback(call.id, "⚠️  sᴇssɪᴏɴ ᴇxᴘᴀɪʀ. ᴅᴏʙᴀʀᴀ sᴇᴀʀᴄʜ ᴋᴀʀᴏ.", show_alert=True)
        try:
            p = max(0, int(p_str))
        except ValueError:
            return safe_answer_callback(call.id)
        results = entry["pages"] if isinstance(entry, dict) else entry
        p       = p % len(results)
        try:
            bot.edit_message_text(
                results[p] + WATERMARK,
                call.message.chat.id, call.message.message_id,
                reply_markup=mk_search_done(qid, p, len(results)),
                disable_web_page_preview=True,
                parse_mode="HTML",
            )
        except Exception:
            pass
        safe_answer_callback(call.id)

    # ── None (page counter button) ──
    elif call.data == "none":
        safe_answer_callback(call.id)

    # ── Copy plain text ──
    elif call.data.startswith("copy_"):
        parts = call.data.split("_", 2)
        if len(parts) != 3:
            return safe_answer_callback(call.id)
        _, qid, p_str = parts
        if not _valid_qid(qid):
            return safe_answer_callback(call.id)
        with _cash_lock:
            if qid not in cash_reports:
                return safe_answer_callback(call.id, "⚠️  sᴇssɪᴏɴ ᴇxᴘᴀɪʀ.", show_alert=True)
            entry = cash_reports[qid]
        try:
            p       = max(0, int(p_str))
            results = entry["pages"] if isinstance(entry, dict) else entry
            plain   = re.sub(r"<[^>]+>", "", results[p % len(results)])[:2000]
            safe_send_message(
                call.message.chat.id,
                f"📋  <b>ᴄᴏᴘʏ ᴋᴀʀᴇɴ:</b>\n\n<code>{html.escape(plain.strip())}</code>",
            )
            safe_answer_callback(call.id, "✅  ᴄᴏᴘɪᴇᴅ!")
        except Exception:
            safe_answer_callback(call.id)

    # ── Ban confirm ──
    elif call.data.startswith("ban_confirm_"):
        if not is_admin(uid):
            return safe_answer_callback(call.id)
        raw_target = call.data.replace("ban_confirm_", "", 1)
        try:
            target = int(raw_target)
        except ValueError:
            return safe_answer_callback(call.id, "⚠️  Invalid.", show_alert=True)
        ban_user(target)
        safe_edit_message(
            f"🚫  <code>{target}</code>  ʙᴀɴ ʜᴏ ɢᴀʏᴀ.",
            call.message.chat.id, call.message.message_id,
        )
        safe_answer_callback(call.id, "✅ ᴅᴏɴᴇ")

    # ── Giveall confirm ──
    elif call.data.startswith("giveall_confirm_"):
        if not is_admin(uid):
            return safe_answer_callback(call.id)
        raw_amt = call.data.replace("giveall_confirm_", "", 1)
        try:
            amt = int(raw_amt)
            if amt <= 0 or amt > 100000:
                raise ValueError
        except ValueError:
            return safe_answer_callback(call.id, "⚠️  Invalid amount.", show_alert=True)
        if give_all_credits(amt):
            safe_edit_message(
                f"🎁  <b>ᴅᴏɴᴇ!</b>  sᴀʙᴋᴏ <code>+{amt} ᴄʀᴇᴅɪᴛs</code> ᴍɪʟ ɢᴀʏᴇ.",
                call.message.chat.id, call.message.message_id,
            )
        safe_answer_callback(call.id)

    # ── Broadcast confirm ──
    elif call.data.startswith("bc_confirm_"):
        if not is_admin(uid):
            return safe_answer_callback(call.id)
        remainder = call.data[len("bc_confirm_"):]
        last_sep  = remainder.rfind("_")
        if last_sep == -1:
            return safe_answer_callback(call.id, "⚠️ Malformed.", show_alert=True)
        try:
            src_chat = int(remainder[:last_sep])
            orig_mid = int(remainder[last_sep + 1:])
        except ValueError:
            return safe_answer_callback(call.id, "⚠️ Malformed.", show_alert=True)

        safe_edit_message(
            "📡  <b>ʙʀᴏᴀᴅᴄᴀsᴛ ᴄʜᴀʟ ʀᴀʜᴀ ʜᴀɪ...</b>",
            call.message.chat.id, call.message.message_id,
        )
        safe_answer_callback(call.id)

        def _do_broadcast():
            users  = get_all_users()
            s = f = 0
            for u in users:
                try:
                    bot.copy_message(u[0], src_chat, orig_mid)
                    s += 1
                    time.sleep(0.05)
                except ApiTelegramException as exc:
                    if exc.error_code == 429:
                        time.sleep(_parse_retry_after(str(exc)))
                        try:
                            bot.copy_message(u[0], src_chat, orig_mid)
                            s += 1
                        except Exception:
                            f += 1
                    elif exc.error_code in (400, 403):
                        f += 1
                    else:
                        logger.warning("[BROADCAST] uid=%s err=%s", u[0], exc)
                        f += 1
                except Exception as exc:
                    logger.warning("[BROADCAST] uid=%s err=%s", u[0], exc)
                    f += 1
            safe_send_message(
                call.message.chat.id,
                f"✅  <b>ʙʀᴏᴀᴅᴄᴀsᴛ ᴅᴏɴᴇ</b>\n<i>{DIV}</i>\n"
                f"📤  sᴇɴᴛ    ›  <code>{s}</code>\n"
                f"❌  ғᴀɪʟᴇᴅ  ›  <code>{f}</code>",
            )

        threading.Thread(target=_do_broadcast, daemon=True, name="broadcast").start()

    # ── Cancel ──
    elif call.data in ("bc_cancel", "admin_cancel"):
        if not is_admin(uid):
            return safe_answer_callback(call.id)
        safe_edit_message(
            "❌  ᴄᴀɴᴄᴇʟ ʜᴏ ɢᴀʏᴀ.",
            call.message.chat.id, call.message.message_id,
        )
        safe_answer_callback(call.id)

    else:
        safe_answer_callback(call.id)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  LAUNCH
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if __name__ == "__main__":
    logger.info("🚀 LaceraOsint starting — daily_limit=%d cooldown=%ds auto_delete=%ds",
                DAILY_LIMIT, COOLDOWN_SECONDS, AUTO_DELETE_SECS)
    _consecutive_errors = 0
    _MAX_CONSECUTIVE    = 10

    while True:
        try:
            bot.infinity_polling(skip_pending=True, timeout=60, long_polling_timeout=30)
            _consecutive_errors = 0

        except ApiTelegramException as exc:
            _consecutive_errors = 0
            if exc.error_code == 409:
                logger.error("[POLL] 409 Conflict — another instance. Waiting 30s…")
                time.sleep(30)
            elif exc.error_code == 429:
                retry = _parse_retry_after(str(exc), default=10)
                logger.warning("[POLL] 429 rate limit. Retry in %ds", retry)
                time.sleep(retry)
            elif exc.error_code in (502, 503, 504):
                wait = random.uniform(8, 15)
                logger.warning("[POLL] %d gateway error. Retry in %.1fs", exc.error_code, wait)
                time.sleep(wait)
            else:
                logger.error("[POLL] ApiTelegramException: %s", exc)
                time.sleep(5)

        except Exception as exc:
            _consecutive_errors += 1
            logger.error("[POLL] crash #%d: %s", _consecutive_errors, exc, exc_info=True)
            if _consecutive_errors >= _MAX_CONSECUTIVE:
                logger.critical("[POLL] %d consecutive crashes — halting.", _consecutive_errors)
                break
            time.sleep(min(5 * _consecutive_errors, 60))
