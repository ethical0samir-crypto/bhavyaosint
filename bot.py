"""
bot.py — LaceraOSINT Telegram Bot
Complete rewrite: thread-safe, production-ready, fully featured.

Fixed:
- ThreadedConnectionPool (was SimpleConnectionPool)
- TOCTOU race condition on credits (atomic DB deduction)
- BOT_SHUTDOWN now uses threading.Event (was bare bool)
- is_joined() silent failure now correctly denies on error
- broadcast runs in background thread (was blocking callback)
- All print() → logging
- /makecode supports s/m/h suffixes + flexible parsing
- /usedcode shows who redeemed a code
- Country flag + name shown in search results
- Auto-delete warning message before deletion
- Aesthetic UI upgrades throughout
"""

import os
import time
import random
import string
import threading
import logging
import html
import re
import datetime
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
)
from api import perform_lookup, detect_country

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  LOGGING SETUP
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

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN env not set!")

OWNER_IDS = [
    int(x.strip()) for x in os.getenv("OWNER_ID", "0").split(",")
    if x.strip().isdigit()
]
ADMIN_USERNAME    = os.getenv("ADMIN_USERNAME", "dissector007bot")
REQUIRED_CHANNELS = [
    ch.strip() for ch in
    os.getenv("REQUIRED_CHANNELS", "@MindRupture,@laceraOsint").split(",")
]

DAILY_LIMIT      = 4
COOLDOWN_SECONDS = 5
AUTO_DELETE_SECS = 120   # seconds before result auto-deletes

# ── Aesthetic dividers ──
DIV  = "─" * 26
SDIV = "·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·"

WATERMARK = (
    "\n\n"
    f"<i>{DIV}</i>\n"
    "✦ <b>ʟᴀᴄᴇʀᴀ ᴏsɪɴᴛ</b>  ᴘʀᴇᴍɪᴜᴍ ɪɴᴛᴇʟʟɪɢᴇɴᴄᴇ\n"
    "⚡ <a href='https://t.me/NeuroLacera'>@NeuroLacera</a>  ·  "
    "<a href='https://t.me/LaceraOsintBot'>@LaceraOsintBot</a>"
)

# ── Country display map (for search header) ──
COUNTRY_DISPLAY = {
    "India":        "🇮🇳",
    "Pakistan":     "🇵🇰",
    "USA/Canada":   "🇺🇸",
    "USA":          "🇺🇸",
    "Russia":       "🇷🇺",
    "Bangladesh":   "🇧🇩",
    "UK":           "🇬🇧",
    "China":        "🇨🇳",
    "UAE":          "🇦🇪",
    "Saudi Arabia": "🇸🇦",
    "Germany":      "🇩🇪",
    "France":       "🇫🇷",
    "Japan":        "🇯🇵",
    "South Korea":  "🇰🇷",
    "Brazil":       "🇧🇷",
    "Mexico":       "🇲🇽",
    "Australia":    "🇦🇺",
    "Nigeria":      "🇳🇬",
    "Egypt":        "🇪🇬",
    "South Africa": "🇿🇦",
    "Indonesia":    "🇮🇩",
    "Unknown":      "🌍",
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  STATE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_shutdown_event  = threading.Event()   # replaces bare bool — thread-safe
USER_COOLDOWN: dict = {}
cash_reports:  dict = {}
BOT_START_TIME = time.time()

setup_db()
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML", threaded=True)

# Cache bot username once at startup — avoid get_me() API call per user request
try:
    _BOT_USERNAME = _BOT_USERNAME
except Exception:
    _BOT_USERNAME = "LaceraOsintBot"  # fallback
logger.info("Bot username: @%s", _BOT_USERNAME)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ACCESS CONTROL
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def is_joined(uid: int) -> bool:
    if uid in OWNER_IDS:
        return True
    all_ok = True
    for ch in REQUIRED_CHANNELS:
        try:
            m = bot.get_chat_member(ch, uid)
            if m.status in ("left", "kicked"):
                return False
        except ApiTelegramException as exc:
            # If bot can't check (e.g., not in channel) — deny, don't silently allow
            logger.warning("is_joined check failed for ch=%s uid=%s: %s", ch, uid, exc)
            all_ok = False
        except Exception as exc:
            logger.warning("is_joined unexpected for ch=%s uid=%s: %s", ch, uid, exc)
            all_ok = False
    return all_ok


def check_access(uid: int) -> str:
    """Returns: OK | BANNED | JOIN_REQ"""
    if is_banned(uid):
        return "BANNED"
    if uid in OWNER_IDS:
        return "OK"
    if not is_joined(uid):
        return "JOIN_REQ"
    return "OK"


def gate(message) -> bool:
    """Full access gate. Returns True if allowed."""
    uid = message.from_user.id
    if _shutdown_event.is_set() and uid not in OWNER_IDS:
        safe_reply_to(message,
            f"🔴  <b>sʏsᴛᴇᴍ ᴏғғʟɪɴᴇ</b>\n"
            f"<i>{DIV}</i>\n"
            "ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ ɪɴ ᴘʀᴏɢʀᴇss.\n"
            "📢  ᴜᴘᴅᴀᴛᴇs: @LaceraOsint"
        )
        return False
    status = check_access(uid)
    if status == "BANNED":
        safe_reply_to(message,
            "🚫  <b>ᴀᴄᴄᴇss ʀᴇᴠᴏᴋᴇᴅ</b>\n"
            "<i>ʏᴏᴜʀ ᴀᴄᴄᴏᴜɴᴛ ʜᴀs ʙᴇᴇɴ sᴜsᴘᴇɴᴅᴇᴅ.</i>"
        )
        return False
    if status == "JOIN_REQ":
        bot.send_message(message.chat.id,
            "🔒  <b>ᴀᴄᴄᴇss ʀᴇsᴛʀɪᴄᴛᴇᴅ</b>\n"
            f"<i>{DIV}</i>\n"
            "ᴊᴏɪɴ ᴏᴜʀ ᴄʜᴀɴɴᴇʟs ᴛᴏ ᴜɴʟᴏᴄᴋ ᴀᴄᴄᴇss.",
            reply_markup=mk_join()
        )
        return False
    return True


def alert_admins(err: str, cmd: str, uid: int, username):
    for aid in OWNER_IDS:
        try:
            bot.send_message(aid,
                "🔴  <b>ᴇʀʀᴏʀ ᴀʟᴇʀᴛ</b>\n"
                f"<i>{DIV}</i>\n"
                f"⌨️  ᴄᴍᴅ  ›  <code>/{cmd}</code>\n"
                f"👤  ᴜsᴇʀ  ›  @{html.escape(str(username or 'N/A'))} "
                f"<code>({uid})</code>\n"
                f"💬  ᴇʀʀ   ›  <code>{html.escape(str(err)[:300])}</code>"
            )
        except Exception:
            pass


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  MARKUP BUILDERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def mk_join() -> InlineKeyboardMarkup:
    mu = InlineKeyboardMarkup()
    for ch in REQUIRED_CHANNELS:
        mu.add(InlineKeyboardButton(
            f"✦  ᴊᴏɪɴ {ch}",
            url=f"https://t.me/{ch.replace('@', '')}"
        ))
    mu.add(InlineKeyboardButton("☑️  ɪ'ᴠᴇ ᴊᴏɪɴᴇᴅ  —  ᴠᴇʀɪғʏ", callback_data="check_join"))
    return mu


def mk_buy() -> InlineKeyboardMarkup:
    mu = InlineKeyboardMarkup(row_width=2)
    mu.add(
        InlineKeyboardButton("💎  ʙᴜʏ ᴄʀᴇᴅɪᴛs", url=f"https://t.me/{ADMIN_USERNAME}"),
        InlineKeyboardButton("✦  ʀᴇғᴇʀ & ᴇᴀʀɴ", callback_data="refer_now")
    )
    return mu


def mk_search_done(qid: str, cur_p: int, total: int) -> InlineKeyboardMarkup:
    mu = InlineKeyboardMarkup(row_width=3)
    if total > 1:
        prev_p = (cur_p - 1) % total
        next_p = (cur_p + 1) % total
        mu.add(
            InlineKeyboardButton("‹", callback_data=f"pg_{qid}_{prev_p}"),
            InlineKeyboardButton(f"◈  {cur_p+1} / {total}", callback_data="none"),
            InlineKeyboardButton("›", callback_data=f"pg_{qid}_{next_p}")
        )
    mu.add(
        InlineKeyboardButton("📋  ᴄᴏᴘʏ", callback_data=f"copy_{qid}_{cur_p}"),
        InlineKeyboardButton("🔄  ɴᴇᴡ sᴇᴀʀᴄʜ", callback_data="new_search"),
        InlineKeyboardButton("◉  ᴘʀᴏғɪʟᴇ", callback_data="my_profile"),
    )
    return mu


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  UTILITIES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def auto_delete_with_warning(chat_id: int, msg_id: int, delay: int = AUTO_DELETE_SECS):
    """Send a countdown warning, then delete the result message."""
    warn_id = None
    try:
        warn_id = bot.send_message(
            chat_id,
            f"⏳  <i>ʏᴇ ʀᴇsᴜʟᴛ  <b>{delay}s</b>  ᴍᴇ ᴀᴜᴛᴏ-ᴅᴇʟᴇᴛᴇ ʜᴏ ᴊᴀᴀᴇɢᴀ.</i>",
        ).message_id
    except Exception:
        pass

    time.sleep(delay)

    for mid in filter(None, [msg_id, warn_id]):
        try:
            bot.delete_message(chat_id, mid)
        except Exception:
            pass


def cache_cleanup():
    while True:
        time.sleep(300)
        now  = time.time()

        # Expire cash_reports older than 10 min
        dead = [
            k for k, v in list(cash_reports.items())
            if isinstance(v, dict) and now - v.get("ts", now) > 600
        ]
        for k in dead:
            cash_reports.pop(k, None)
        if dead:
            logger.info("[CACHE] cleared %d expired report entries", len(dead))

        # Expire USER_COOLDOWN entries older than 1 hour (prevent unbounded growth)
        stale_cd = [k for k, v in list(USER_COOLDOWN.items()) if now - v > 3600]
        for k in stale_cd:
            USER_COOLDOWN.pop(k, None)
        if stale_cd:
            logger.info("[CACHE] cleared %d stale cooldown entries", len(stale_cd))


threading.Thread(target=cache_cleanup, daemon=True).start()


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


def fmt_reset(ts: int) -> str:
    left = ts - int(time.time())
    if left <= 0:
        return "ʀᴇsᴇᴛᴛɪɴɢ ɴᴏᴡ..."
    h, r = divmod(left, 3600)
    m, s = divmod(r, 60)
    return f"{h}h {m}m {s}s"


def fmt_bar(used: int, total: int) -> str:
    used   = max(0, min(used, total))
    filled = "█" * used
    empty  = "░" * (total - used)
    pct    = int((used / total) * 100) if total else 0
    return f"[{filled}{empty}]  {pct}%"


def fmt_expiry(ts) -> str:
    """Format expiry timestamp to human-readable."""
    if ts is None:
        return "ɴᴇᴠᴇʀ"
    left = int(ts) - int(time.time())
    if left <= 0:
        return "ᴇxᴘɪʀᴇᴅ ✗"
    if left < 60:
        return f"{left}s"
    if left < 3600:
        return f"{left // 60}m {left % 60}s"
    if left < 86400:
        h = left // 3600
        m = (left % 3600) // 60
        return f"{h}h {m}m"
    d = left // 86400
    h = (left % 86400) // 3600
    return f"{d}d {h}h"


def fmt_ts(ts) -> str:
    """Format unix timestamp to readable date."""
    if not ts:
        return "N/A"
    return datetime.datetime.fromtimestamp(int(ts)).strftime("%d/%m/%y %H:%M")


def parse_duration(raw: str):
    """
    Parse flexible duration string to seconds.
    Supports: 10s, 10m, 10h, 10d, 10 (treated as minutes by default)
    Also: 1h30m, 2d12h etc.
    Returns seconds or None if invalid.
    """
    raw = raw.strip().lower()
    if not raw:
        return None

    total = 0
    pattern = re.findall(r'(\d+)\s*([smhd]?)', raw)
    found_any = False

    for num_str, unit in pattern:
        if not num_str:
            continue
        n = int(num_str)
        if n == 0:
            continue
        if unit == 's':
            total += n
        elif unit == 'm' or unit == '':
            total += n * 60
        elif unit == 'h':
            total += n * 3600
        elif unit == 'd':
            total += n * 86400
        else:
            # unrecognised unit — treat as minutes
            total += n * 60
        found_any = True

    return total if found_any and total > 0 else None


def is_admin(uid: int) -> bool:
    return uid in OWNER_IDS


def normalize_phone(raw: str) -> tuple:
    """
    Normalize any international phone number.
    Returns (e164_digits, country_code_str) or ("", "") on failure.
    E164 = full number with country code, no + sign. e.g. "919876543210"

    Accepts:
      +91 9876543210   → ("919876543210", "91")
      +1 555 123 4567  → ("15551234567",  "1")
      00923001234567   → ("923001234567", "92")
      9876543210       → ("919876543210", "91")  ← bare 10-digit = assume India
    """
    # Strip all non-digits except leading +
    clean = re.sub(r'[\s\-\(\)\.]', '', raw.strip())

    # Handle 00-prefix international format (e.g. 0092...)
    if clean.startswith("00") and len(clean) > 4:
        clean = "+" + clean[2:]

    if clean.startswith("+"):
        digits = clean[1:]
        if digits.isdigit() and 7 <= len(digits) <= 15:
            # Extract country code (try 1, 2, 3 digit prefixes)
            cc = _extract_country_code(digits)
            return (digits, cc)
        return ("", "")

    # Pure digits
    if not clean.isdigit():
        return ("", "")

    # Bare 10-digit → assume India
    if len(clean) == 10:
        return (f"91{clean}", "91")

    # 11-digit starting with 0 → strip trunk 0, assume India
    if len(clean) == 11 and clean.startswith("0"):
        return (f"91{clean[1:]}", "91")

    # Already has country code (11–15 digits)
    if 11 <= len(clean) <= 15:
        cc = _extract_country_code(clean)
        return (clean, cc)

    return ("", "")


# Country code prefix map — longest match wins
_CC_PREFIXES = [
    # 3-digit CCs first (longest match priority)
    ("880", "BD"), ("998", "UZ"), ("971", "AE"), ("966", "SA"),
    ("965", "KW"), ("964", "IQ"), ("963", "SY"), ("962", "JO"),
    ("961", "LB"), ("960", "MV"), ("977", "NP"), ("976", "MN"),
    ("975", "BT"), ("974", "QA"), ("973", "BH"), ("972", "IL"),
    ("970", "PS"), ("968", "OM"), ("967", "YE"), ("856", "LA"),
    ("855", "KH"), ("853", "MO"), ("852", "HK"), ("850", "KP"),
    ("886", "TW"), ("380", "UA"), ("375", "BY"), ("374", "AM"),
    ("373", "MD"), ("372", "EE"), ("371", "LV"), ("370", "LT"),
    ("358", "FI"), ("357", "CY"), ("356", "MT"), ("354", "IS"),
    ("353", "IE"), ("352", "LU"), ("351", "PT"), ("350", "GI"),
    ("299", "GL"), ("298", "FO"), ("297", "AW"), ("264", "NA"),
    ("263", "ZW"), ("262", "RE"), ("261", "MG"), ("260", "ZM"),
    ("258", "MZ"), ("257", "BI"), ("256", "UG"), ("255", "TZ"),
    ("254", "KE"), ("253", "DJ"), ("252", "SO"), ("251", "ET"),
    ("250", "RW"), ("249", "SD"), ("248", "SC"), ("246", "IO"),
    ("245", "GW"), ("244", "AO"), ("243", "CD"), ("242", "CG"),
    ("241", "GA"), ("240", "GQ"), ("239", "ST"), ("238", "CV"),
    ("237", "CM"), ("236", "CF"), ("235", "TD"), ("234", "NG"),
    ("233", "GH"), ("232", "SL"), ("231", "LR"), ("230", "MU"),
    ("229", "BJ"), ("228", "TG"), ("227", "NE"), ("226", "BF"),
    ("225", "CI"), ("224", "GN"), ("223", "ML"), ("222", "MR"),
    ("221", "SN"), ("220", "GM"), ("218", "LY"), ("216", "TN"),
    ("213", "DZ"), ("212", "MA"),
    # 2-digit CCs
    ("92", "PK"), ("91", "IN"), ("90", "TR"), ("86", "CN"),
    ("84", "VN"), ("82", "KR"), ("81", "JP"), ("66", "TH"),
    ("65", "SG"), ("64", "NZ"), ("63", "PH"), ("62", "ID"),
    ("61", "AU"), ("60", "MY"), ("58", "VE"), ("57", "CO"),
    ("56", "CL"), ("55", "BR"), ("54", "AR"), ("52", "MX"),
    ("51", "PE"), ("49", "DE"), ("48", "PL"), ("47", "NO"),
    ("46", "SE"), ("45", "DK"), ("44", "GB"), ("43", "AT"),
    ("41", "CH"), ("40", "RO"), ("39", "IT"), ("38", ""),
    ("36", "HU"), ("34", "ES"), ("33", "FR"), ("32", "BE"),
    ("31", "NL"), ("30", "GR"), ("27", "ZA"), ("20", "EG"),
    # 1-digit CCs
    ("7", "RU"), ("1", "US"),
]

def _extract_country_code(digits: str) -> str:
    """Extract country code string from E164 digits (no +)."""
    for prefix, _ in _CC_PREFIXES:
        if digits.startswith(prefix):
            return prefix
    return ""


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SAFE TELEGRAM WRAPPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def safe_reply_to(message, text, **kwargs):
    try:
        return bot.reply_to(message, text, **kwargs)
    except ApiTelegramException as exc:
        if "message to be replied not found" in str(exc) or exc.error_code == 400:
            try:
                return bot.send_message(message.chat.id, text, **kwargs)
            except Exception:
                pass
        elif exc.error_code == 429:
            retry = 5
            try:
                retry = int(str(exc).split("retry after ")[-1])
            except Exception:
                pass
            time.sleep(retry)
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
    except Exception:
        pass
    return None


def safe_send_message(chat_id, text, **kwargs):
    for attempt in range(3):
        try:
            return bot.send_message(chat_id, text, **kwargs)
        except ApiTelegramException as exc:
            if exc.error_code == 429:
                retry = 5
                try:
                    retry = int(str(exc).split("retry after ")[-1])
                except Exception:
                    pass
                time.sleep(retry)
            elif exc.error_code in (502, 503, 504):
                time.sleep(3 * (attempt + 1))
            else:
                return None
        except Exception:
            time.sleep(2)
    return None


def safe_answer_callback(call_id, text=None, show_alert=False):
    try:
        bot.answer_callback_query(call_id, text, show_alert=show_alert)
    except ApiTelegramException as exc:
        if "query is too old" in str(exc) or "query ID is invalid" in str(exc):
            pass
        elif exc.error_code == 429:
            time.sleep(5)
            try:
                bot.answer_callback_query(call_id, text, show_alert=show_alert)
            except Exception:
                pass
    except Exception:
        pass


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  USER COMMANDS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.message_handler(commands=["start"])
def cmd_start(message):
    if not gate(message):
        return

    uid        = message.from_user.id
    username   = message.from_user.username or None
    first_name = message.from_user.first_name or None
    # Process referral BEFORE get_user — add_referral checks user doesn't exist yet
    # If we call get_user first, it creates the user and referral always fails
    args = message.text.split()
    referral_processed = False
    if len(args) > 1 and args[1].isdigit():
        ref_id = int(args[1])
        if ref_id != uid and add_referral(uid, ref_id):
            referral_processed = True
            try:
                bot.send_message(ref_id,
                    "🎁  <b>ʀᴇғᴇʀʀᴀʟ ʙᴏɴᴜs</b>\n"
                    f"<i>{DIV}</i>\n"
                    "ᴀ ɴᴇᴡ ᴏᴘᴇʀᴀᴛɪᴠᴇ ᴊᴏɪɴᴇᴅ ᴠɪᴀ ʏᴏᴜʀ ʟɪɴᴋ.\n"
                    "✦  <b>+2 ᴄʀᴇᴅɪᴛs</b> ᴄʀᴇᴅɪᴛᴇᴅ ᴛᴏ ʏᴏᴜʀ ᴀᴄᴄᴏᴜɴᴛ."
                )
            except Exception:
                pass

    # Ensure user exists (add_referral creates them if referral succeeded)
    get_user(uid)
    update_user_info(uid, username, first_name)

    name = html.escape(message.from_user.first_name or "Operative")
    bot.send_message(message.chat.id,
        "╔══════════════════════════\n"
        "  ✦  <b>ʟᴀᴄᴇʀᴀ ᴏsɪɴᴛ</b>  —  ᴅᴇᴇᴘ ɪɴᴛᴇʟʟɪɢᴇɴᴄᴇ\n"
        "╚══════════════════════════\n\n"
        f"ᴡᴇʟᴄᴏᴍᴇ, <b>{name}</b>.\n"
        "ᴍᴜʟᴛɪ-ʟᴀʏᴇʀ ᴅᴀᴛᴀ ɪɴᴛᴇʟʟɪɢᴇɴᴄᴇ ᴀᴛ ʏᴏᴜʀ ᴄᴏᴍᴍᴀɴᴅ use /help to check all commands.\n\n"
        "◈  <b>sᴇᴀʀᴄʜ ᴍᴏᴅᴇs</b>\n"
        f"<i>{DIV}</i>\n"
        "  📞  /number   <code>+91/+92/+1/+44...</code>\n"
        "  📧  /email    <code>user@mail.com</code>\n"
        "  🪪  /aadhar   <code>XXXXXXXXXXXX</code>\n"
        "  💳  /pan      <code>ABCDE1234F</code>\n"
        "  🚗  /vehicle  <code>MH12AB1234</code>\n"
        "  🌐  /ip       <code>1.2.3.4</code>\n\n"
        "◈  <b>ᴀᴄᴄᴏᴜɴᴛ</b>\n"
        f"<i>{DIV}</i>\n"
        "  📊  /profile  ·  ✦  /refer  ·  🎫  /redeem\n\n"
        f"<i>⚡  <a href='https://t.me/NeuroLacera'>@NeuroLacera</a>  ·  "
        f"<a href='https://t.me/LaceraOsintBot'>@LaceraOsintBot</a></i>"
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
        "◈  <b>sᴇᴀʀᴄʜ ᴍᴏᴅᴇs</b>\n"
        f"<i>{DIV}</i>\n"
        "  📞  <b>/number</b>  <code>+CC XXXXXXXXX</code>\n"
        "  📧  <b>/email</b>   <code>user@domain.com</code>\n"
        "  🪪  <b>/aadhar</b>  <code>XXXX XXXX XXXX</code>\n"
        "  💳  <b>/pan</b>     <code>ABCDE1234F</code>\n"
        "  🚗  <b>/vehicle</b> <code>MH12AB1234</code>\n"
        "  🌐  <b>/ip</b>      <code>1.2.3.4</code>\n\n"
        "◈  <b>📞 sᴜᴘᴘᴏʀᴛᴇᴅ ᴄᴏᴜɴᴛʀɪᴇs  (ɴᴜᴍʙᴇʀ)</b>\n"
        f"<i>{DIV}</i>\n"
        "  🇮🇳 India      <code>+91 XXXXX XXXXX</code>\n"
        "  🇵🇰 Pakistan   <code>+92 3XX XXXXXXX</code>\n"
        "  🇺🇸 USA/Canada <code>+1 XXX XXX XXXX</code>\n"
        "  🇷🇺 Russia     <code>+7 XXX XXX XXXX</code>\n"
        "  🇧🇩 Bangladesh <code>+880 1XXX XXXXXX</code>\n"
        "  🇬🇧 UK         <code>+44 7XXX XXXXXX</code>\n"
        "  🇨🇳 China      <code>+86 1XX XXXX XXXX</code>\n"
        "  🇦🇪 UAE        <code>+971 5X XXX XXXX</code>\n"
        "  🇸🇦 Saudi      <code>+966 5X XXX XXXX</code>\n"
        "  🇩🇪 Germany    <code>+49 1XX XXXXXXX</code>\n"
        "  🇫🇷 France     <code>+33 6XX XXX XXX</code>\n"
        "  🇯🇵 Japan      <code>+81 9X XXXX XXXX</code>\n"
        "  🇧🇷 Brazil     <code>+55 XX XXXXX XXXX</code>\n"
        "  🇮🇩 Indonesia  <code>+62 8XX XXX XXXX</code>\n"
        "  🇳🇬 Nigeria    <code>+234 7XX XXX XXXX</code>\n"
        "  🌍 <i>ᴀɴʏ ᴄᴏᴜɴᴛʀʏ ᴡɪᴛʜ</i> <code>+CC</code> <i>ᴡᴏʀᴋs</i>\n\n"
        "◈  <b>ᴀᴄᴄᴏᴜɴᴛ</b>\n"
        f"<i>{DIV}</i>\n"
        "  📊  /profile  —  sᴛᴀᴛs & ᴄʀᴇᴅɪᴛs\n"
        "  ✦   /refer    —  ᴇᴀʀɴ 2 ᴄʀᴇᴅɪᴛs ᴘᴇʀ ɪɴᴠɪᴛᴇ\n"
        "  🎫  /redeem   —  ᴄʟᴀɪᴍ ᴘʀᴏᴍᴏ ᴄᴏᴅᴇ\n\n"
        "◈  <b>ʜᴏᴡ ɪᴛ ᴡᴏʀᴋs</b>\n"
        f"  ·  {DAILY_LIMIT} ғʀᴇᴇ sᴇᴀʀᴄʜᴇs ᴘᴇʀ ᴅᴀʏ\n"
        "  ·  ᴀғᴛᴇʀ ᴛʜᴀᴛ: 1 ᴄʀᴇᴅɪᴛ ᴘᴇʀ sᴇᴀʀᴄʜ\n"
        f"  ·  ʀᴇsᴜʟᴛs ᴀᴜᴛᴏ-ᴅᴇʟᴇᴛᴇ ɪɴ {AUTO_DELETE_SECS}s"
    )

    if is_own:
        text += (
            f"\n\n<i>{DIV}</i>\n"
            "👑  <b>ᴀᴅᴍɪɴ ᴘᴀɴᴇʟ</b>\n"
            "  /ownerbot  /stats  /ping  /broadcast\n"
            "  /makecode  /usedcode  /giveall  /addcredits\n"
            "  /userlist  /userinfo  /detail\n"
            "  /ban  /unban  /lock  /unlock\n"
            "  /listlocked  /shutdown"
        )

    bot.send_message(message.chat.id, text)


@bot.message_handler(commands=["profile"])
def cmd_profile(message):
    if not gate(message):
        return

    uid = message.from_user.id
    u   = get_user(uid)
    if not u:
        return safe_reply_to(message, "❌  ᴇʀʀᴏʀ ʟᴏᴀᴅɪɴɢ ᴘʀᴏғɪʟᴇ.")

    credits     = u[1]
    daily_used  = u[2]
    daily_reset = u[3]
    refer_count = u[5]

    remaining = max(0, DAILY_LIMIT - daily_used)
    bar       = fmt_bar(daily_used, DAILY_LIMIT)
    reset_in  = fmt_reset(daily_reset)

    bot.send_message(message.chat.id,
        "╔══════════════════════════\n"
        "  ◉  <b>ᴏᴘᴇʀᴀᴛɪᴠᴇ ᴘʀᴏғɪʟᴇ</b>\n"
        "╚══════════════════════════\n"
        f"🆔  ɪᴅ         <code>{uid}</code>\n"
        f"📡  sᴛᴀᴛᴜs    🟢 ᴀᴄᴛɪᴠᴇ\n"
        f"💎  ᴄʀᴇᴅɪᴛs   <code>{credits}</code>\n\n"
        "◈  <b>ᴅᴀɪʟʏ ᴜsᴀɢᴇ</b>\n"
        f"  {bar}\n"
        f"  🆓  ʀᴇᴍᴀɪɴɪɴɢ  ›  <code>{remaining} / {DAILY_LIMIT}</code>\n"
        f"  ⏱️  ʀᴇsᴇᴛ ɪɴ    ›  <code>{reset_in}</code>\n\n"
        f"<i>{DIV}</i>\n"
        f"✦  ʀᴇғᴇʀʀᴀʟs   <code>{refer_count}</code>\n"
        "<i>ᴜsᴇ /refer ᴛᴏ ᴇᴀʀɴ ᴍᴏʀᴇ ᴄʀᴇᴅɪᴛs</i>",
        reply_markup=mk_buy()
    )


@bot.message_handler(commands=["refer"])
def cmd_refer(message):
    if not gate(message):
        return

    uid   = message.from_user.id
    u     = get_user(uid)
    count = u[5] if u else 0
    link  = f"https://t.me/{_BOT_USERNAME}?start={uid}"

    bot.send_message(message.chat.id,
        "✦  <b>ʀᴇғᴇʀʀᴀʟ ᴘʀᴏɢʀᴀᴍ</b>\n"
        f"<i>{DIV}</i>\n"
        "ɪɴᴠɪᴛᴇ ᴏᴘᴇʀᴀᴛɪᴠᴇs & ᴇᴀʀɴ ᴄʀᴇᴅɪᴛs ᴀᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ.\n\n"
        "🎁  ʀᴇᴡᴀʀᴅ  ›  <b>+2 ᴄʀᴇᴅɪᴛs</b> ᴘᴇʀ ɴᴇᴡ ᴜsᴇʀ\n\n"
        "🔗  <b>ʏᴏᴜʀ ʟɪɴᴋ</b>\n"
        f"<code>{link}</code>\n\n"
        f"<i>{DIV}</i>\n"
        f"👥  ᴛᴏᴛᴀʟ ʀᴇғᴇʀʀᴀʟs  ›  <code>{count}</code>"
    )


@bot.message_handler(commands=["redeem"])
def cmd_redeem(message):
    if not gate(message):
        return

    args = message.text.split()
    if len(args) < 2:
        return safe_reply_to(message,
            "🎫  <b>ᴜsᴀɢᴇ</b>\n"
            "<code>/redeem YOUR-CODE</code>"
        )

    result = redeem_code(message.from_user.id, args[1].strip().upper())

    if result is None:
        safe_reply_to(message, "❌  <b>ɪɴᴠᴀʟɪᴅ ᴄᴏᴅᴇ.</b>")
    elif result == -1:
        safe_reply_to(message, "⚠️  <b>ᴄᴏᴅᴇ ᴀʟʀᴇᴀᴅʏ ᴜsᴇᴅ.</b>")
    elif result == -2:
        safe_reply_to(message, "⏳  <b>ᴄᴏᴅᴇ ᴇxᴘɪʀᴇ ʜᴏ ɢᴀʏᴀ.</b>")
    else:
        safe_reply_to(message,
            "✅  <b>ʀᴇᴅᴇᴇᴍᴇᴅ sᴜᴄᴄᴇssғᴜʟʟʏ</b>\n"
            f"<i>{DIV}</i>\n"
            f"💎  <b>+{result} ᴄʀᴇᴅɪᴛs</b> ᴀᴅᴅᴇᴅ ᴛᴏ ʏᴏᴜʀ ᴀᴄᴄᴏᴜɴᴛ."
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SEARCH SYSTEM
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.message_handler(commands=["number", "email", "vehicle", "aadhar", "pan", "ip"])
def cmd_lookup(message):
    if not gate(message):
        return

    raw_cmd = message.text.split()[0][1:]
    cmd     = raw_cmd.split("@")[0].lower()
    args    = message.text.split(maxsplit=1)

    MODE_EMOJI = {
        "number": "📞", "email": "📧", "aadhar": "🪪",
        "pan": "💳", "vehicle": "🚗", "ip": "🌐"
    }
    emoji = MODE_EMOJI.get(cmd, "🔍")

    if len(args) < 2:
        sent = safe_reply_to(message,
            f"{emoji}  <b>ᴇɴᴛᴇʀ {cmd.upper()} ᴛᴏ sᴇᴀʀᴄʜ</b>"
        )
        if sent:
            bot.register_next_step_handler(sent, lambda m, c=cmd: do_search(m, c))
    else:
        do_search(message, cmd, args[1].strip())


def do_search(message, mode: str, query: str = None):
    uid = message.from_user.id

    # Stale message guard — if triggered from next_step handler after >5 min, ignore
    if query is None:  # came from next_step handler, not direct call
        msg_age = time.time() - message.date
        if msg_age > 300:  # 5 minutes
            return safe_reply_to(message,
                "⏱️  <b>sᴇssɪᴏɴ ᴛɪᴍᴇᴅ ᴏᴜᴛ.</b>  ᴅᴏʙᴀʀᴀ ᴄᴏᴍᴍᴀɴᴅ ᴜsᴇ ᴋᴀʀᴏ."
            )

    if _shutdown_event.is_set() and uid not in OWNER_IDS:
        return safe_reply_to(message,
            "🔴  <b>sʏsᴛᴇᴍ ᴏғғʟɪɴᴇ</b>  —  ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ ɪɴ ᴘʀᴏɢʀᴇss."
        )

    access = check_access(uid)
    if access == "BANNED":
        return safe_reply_to(message, "🚫  <b>ᴀᴄᴄᴇss ʀᴇᴠᴏᴋᴇᴅ.</b>")
    if access == "JOIN_REQ":
        return bot.send_message(message.chat.id,
            "🔒  <b>ᴀᴄᴄᴇss ʀᴇsᴛʀɪᴄᴛᴇᴅ</b>\n"
            f"<i>{DIV}</i>\n"
            "ᴊᴏɪɴ ᴏᴜʀ ᴄʜᴀɴɴᴇʟs ᴛᴏ ᴜɴʟᴏᴄᴋ ᴀᴄᴄᴇss.",
            reply_markup=mk_join()
        )

    try:
        update_user_info(uid,
            message.from_user.username or None,
            message.from_user.first_name or None
        )
    except Exception:
        pass

    raw_q = (query or message.text or "").strip()

    if raw_q.startswith("/"):
        safe_reply_to(message, "↩️  <b>ᴄᴀɴᴄᴇʟʟᴇᴅ.</b>  ᴘʀᴏᴄᴇssɪɴɢ ɴᴇᴡ ᴄᴏᴍᴍᴀɴᴅ...")
        return bot.process_new_messages([message])

    if not raw_q:
        return safe_reply_to(message, "❌  ᴇᴍᴘᴛʏ ǫᴜᴇʀʏ.")

    # Cooldown
    if uid not in OWNER_IDS:
        now  = time.time()
        last = USER_COOLDOWN.get(uid, 0)
        if now - last < COOLDOWN_SECONDS:
            left = int(COOLDOWN_SECONDS - (now - last)) + 1
            return safe_reply_to(message,
                f"⏳  <b>ᴄᴏᴏʟᴅᴏᴡɴ</b>  ›  <code>{left}s</code> ʀᴇᴍᴀɪɴɪɴɢ."
            )
        USER_COOLDOWN[uid] = now

    # Phone validation
    if mode == "number":
        e164, cc = normalize_phone(raw_q)
        if not e164:
            return safe_reply_to(message,
                "❌  <b>ɪɴᴠᴀʟɪᴅ ɴᴜᴍʙᴇʀ</b>\n\n"
                "◈  <b>ᴀᴄᴄᴇᴘᴛᴇᴅ ғᴏʀᴍᴀᴛs:</b>\n"
                "  🇮🇳  <code>+91 98765 43210</code>\n"
                "  🇵🇰  <code>+92 300 1234567</code>\n"
                "  🇺🇸  <code>+1 555 123 4567</code>\n"
                "  🇬🇧  <code>+44 7911 123456</code>\n"
                "  🇧🇩  <code>+880 1712 345678</code>\n"
                "  🌍  <i>ᴋᴏɪ ʙʜɪ ᴄᴏᴜɴᴛʀʏ ᴋᴀ <code>+ᴄᴄ ɴᴜᴍʙᴇʀ</code> ᴅᴀᴀʟᴏ</i>"
            )
        q = e164
    else:
        q = raw_q

    # Credit check
    user = get_user(uid)
    if not user:
        return safe_reply_to(message, "❌  ᴇʀʀᴏʀ ʟᴏᴀᴅɪɴɢ ᴜsᴇʀ ᴅᴀᴛᴀ.")

    credits    = user[1]
    daily_used = user[2]

    if daily_used >= DAILY_LIMIT and credits <= 0:
        return safe_reply_to(message,
            "⚠️  <b>ʟɪᴍɪᴛ ᴇxʜᴀᴜsᴛᴇᴅ</b>\n"
            f"<i>{DIV}</i>\n"
            f"ᴅᴀɪʟʏ ʟɪᴍɪᴛ: <code>{DAILY_LIMIT}</code>  ·  ᴄʀᴇᴅɪᴛs: <code>0</code>\n\n"
            "ʙᴜʏ ᴄʀᴇᴅɪᴛs ᴏʀ ʀᴇғᴇʀ ᴜsᴇʀs ᴛᴏ ᴄᴏɴᴛɪɴᴜᴇ.",
            reply_markup=mk_buy()
        )

    # Stealth lock
    if is_query_locked(q):
        time.sleep(random.uniform(2.5, 4.5))
        return bot.send_message(message.chat.id, "🔍  <b>ɴᴏ ʀᴇᴄᴏʀᴅs ғᴏᴜɴᴅ.</b>")

    # Detect country once — pass to perform_lookup to avoid double-call
    flag, country_name = detect_country(q, mode)
    country_line = f"{flag} <i>{html.escape(country_name)}</i>  ·  " if country_name != "Unknown" else ""

    # Searching indicator
    wait = bot.send_message(message.chat.id,
        "╔══════════════════════════\n"
        f"  ⟳  <b>sᴄᴀɴɴɪɴɢ ᴅᴀᴛᴀʙᴀsᴇs</b>\n"
        "╚══════════════════════════\n"
        f"  {country_line}<code>{html.escape(q)}</code>\n"
        "  <i>ᴘʟᴇᴀsᴇ ᴡᴀɪᴛ...</i>"
    )

    try:
        results, _ = perform_lookup(q, mode)  # country already detected above

        try:
            bot.delete_message(message.chat.id, wait.message_id)
        except Exception:
            pass

        if not results or not isinstance(results, list):
            return bot.send_message(message.chat.id, "🔍  <b>ɴᴏ ʀᴇᴄᴏʀᴅs ғᴏᴜɴᴅ.</b>")

        first   = results[0]
        is_real = not any(first.lstrip().startswith(p) for p in (
            "🔍", "❌", "⚠️", "⏱️", "🌐",
            "<b>ɴᴏ", "<b>ᴀᴘɪ", "<b>ɪɴᴠᴀʟɪᴅ",
            "<b>sᴇʀᴠᴇʀ", "<b>ɴᴇᴛᴡᴏʀᴋ", "<b>ʀᴇQ",
            "✦  <b>sᴇʀᴠɪᴄᴇ",
        ))

        if is_real:
            # Atomic deduction — fixes TOCTOU race condition
            if not deduct_credit_atomic(uid, DAILY_LIMIT):
                return safe_reply_to(message,
                    "⚠️  <b>ʟɪᴍɪᴛ ᴇxʜᴀᴜsᴛᴇᴅ</b>",
                    reply_markup=mk_buy()
                )
            log_search(uid, q, mode, country_name)

        qid = "".join(random.choices(string.ascii_letters + string.digits, k=8))
        cash_reports[qid] = {"pages": results, "ts": time.time()}
        markup = mk_search_done(qid, 0, len(results))

        try:
            sent = bot.send_message(
                message.chat.id,
                results[0] + WATERMARK,
                reply_markup=markup,
                disable_web_page_preview=True
            )
            # Auto-delete with warning — in background thread
            threading.Thread(
                target=auto_delete_with_warning,
                args=(message.chat.id, sent.message_id, AUTO_DELETE_SECS),
                daemon=True
            ).start()
        except ApiTelegramException:
            plain = re.sub(r'<[^>]+>', '', results[0])
            bot.send_message(
                message.chat.id,
                plain + "\n\n⚡ @LaceraOsintBot",
                reply_markup=markup
            )

    except Exception as exc:
        try:
            bot.delete_message(message.chat.id, wait.message_id)
        except Exception:
            pass
        bot.send_message(message.chat.id,
            "⚠️  <b>ᴋᴜᴄʜ ᴇʀʀᴏʀ ᴀᴀ ɢᴀʏᴀ</b>\n"
            f"<i>{DIV}</i>\n"
            "ᴛʜᴏᴅɪ ᴅᴇʀ ʙᴀᴀᴅ ᴅᴏʙᴀʀᴀ ᴛʀʏ ᴋᴀʀᴇɴ. 🙏"
        )
        alert_admins(str(exc), mode, uid, message.from_user.username)
        logger.error("do_search error uid=%s mode=%s: %s", uid, mode, exc, exc_info=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ADMIN COMMANDS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.message_handler(commands=["ownerbot"])
def cmd_ownerbot(message):
    if not is_admin(message.from_user.id):
        return
    safe_reply_to(message,
        "╔══════════════════════════\n"
        "  👑  <b>ᴀᴅᴍɪɴ ᴄᴏɴᴛʀᴏʟ ᴘᴀɴᴇʟ</b>\n"
        "╚══════════════════════════\n"
        "  📊  /stats           —  sʏsᴛᴇᴍ ᴏᴠᴇʀᴠɪᴇᴡ\n"
        "  🏓  /ping            —  ʟᴀᴛᴇɴᴄʏ & ᴜᴘᴛɪᴍᴇ\n"
        "  📢  /broadcast       —  ᴍᴀss ᴍᴇssᴀɢᴇ\n"
        f"<i>{DIV}</i>\n"
        "  🎫  /makecode [ᴀᴍᴛ] [ᴛɪᴍᴇ]  —  ɢᴇɴᴇʀᴀᴛᴇ ᴄᴏᴅᴇ\n"
        "       ᴛɪᴍᴇ: 30s / 10m / 2h / 1d\n"
        "  🔍  /usedcode [ᴄᴏᴅᴇ]  —  ᴡʜᴏ ᴜsᴇᴅ ɪᴛ\n"
        "  🎁  /giveall [ᴀᴍᴛ]   —  ᴄʀᴇᴅɪᴛs ᴛᴏ ᴀʟʟ\n"
        "  💰  /addcredits [ᴜɪᴅ] [ᴀᴍᴛ]\n"
        f"<i>{DIV}</i>\n"
        "  👥  /userlist         —  ᴀʟʟ ᴜsᴇʀs\n"
        "  👤  /userinfo [ᴜɪᴅ]   —  ᴘʀᴏғɪʟᴇ\n"
        "  🕵️  /detail [ᴜɪᴅ]     —  sᴇᴀʀᴄʜ ʜɪsᴛᴏʀʏ\n"
        "  🚫  /ban [ᴜɪᴅ]        —  ʙᴀɴ ᴜsᴇʀ\n"
        "  ✅  /unban [ᴜɪᴅ]      —  ᴜɴʙᴀɴ ᴜsᴇʀ\n"
        f"<i>{DIV}</i>\n"
        "  🔒  /lock [ǫ]         —  ʟᴏᴄᴋ ǫᴜᴇʀʏ\n"
        "  🔓  /unlock [ǫ]       —  ᴜɴʟᴏᴄᴋ ǫᴜᴇʀʏ\n"
        "  📋  /listlocked       —  ᴀʟʟ ʟᴏᴄᴋᴇᴅ\n"
        "  🛑  /shutdown [on/off]"
    )


@bot.message_handler(commands=["ping"])
def cmd_ping(message):
    if not is_admin(message.from_user.id):
        return
    t0  = time.time()
    msg = safe_reply_to(message, "⟳  ᴘɪɴɢɪɴɢ...")
    if not msg:
        return
    lat = round((time.time() - t0) * 1000, 1)
    up  = fmt_uptime(time.time() - BOT_START_TIME)
    try:
        bot.edit_message_text(
            "🏓  <b>ᴘᴏɴɢ</b>\n"
            f"<i>{DIV}</i>\n"
            f"⚡  ʟᴀᴛᴇɴᴄʏ  ›  <code>{lat} ms</code>\n"
            f"⏱️  ᴜᴘᴛɪᴍᴇ   ›  <code>{up}</code>\n"
            f"💾  ᴄᴀᴄʜᴇ    ›  <code>{len(cash_reports)} ᴇɴᴛʀɪᴇs</code>",
            message.chat.id, msg.message_id
        ,
                parse_mode="HTML")
    except Exception:
        pass


@bot.message_handler(commands=["stats"])
def cmd_stats(message):
    if not is_admin(message.from_user.id):
        return

    users         = get_all_users_detail()
    total         = len(users)
    banned        = sum(1 for u in users if u[4] == 1)
    locked        = len(get_locked_list())
    up            = fmt_uptime(time.time() - BOT_START_TIME)
    today         = get_today_search_count()
    total_s       = get_total_search_count()
    mode_stats    = get_search_stats_by_mode()
    country_stats = get_search_stats_by_country()

    mode_lines = ""
    for mode, cnt in sorted(mode_stats.items(), key=lambda x: -x[1]):
        mode_lines += f"  ›  {mode.upper():<8}  <code>{cnt}</code>\n"

    country_lines = ""
    for c_name, cnt in country_stats[:5]:
        flag = COUNTRY_DISPLAY.get(c_name, "🌍")
        country_lines += f"  ›  {flag} {html.escape(c_name):<14}  <code>{cnt}</code>\n"

    text = (
        "╔══════════════════════════\n"
        "  📊  <b>sʏsᴛᴇᴍ sᴛᴀᴛɪsᴛɪᴄs</b>\n"
        "╚══════════════════════════\n"
        f"👥  ᴛᴏᴛᴀʟ ᴜsᴇʀs       ›  <code>{total}</code>\n"
        f"🚫  ʙᴀɴɴᴇᴅ            ›  <code>{banned}</code>\n"
        f"🟢  ᴀᴄᴛɪᴠᴇ            ›  <code>{total - banned}</code>\n"
        f"🔒  ʟᴏᴄᴋᴇᴅ ǫᴜᴇʀɪᴇs   ›  <code>{locked}</code>\n"
        f"<i>{DIV}</i>\n"
        f"🔍  ᴀᴀᴊ ᴋɪ sᴇᴀʀᴄʜᴇs  ›  <code>{today}</code>\n"
        f"📈  ᴛᴏᴛᴀʟ sᴇᴀʀᴄʜᴇs   ›  <code>{total_s}</code>\n"
        f"💾  ᴄᴀᴄʜᴇ ᴇɴᴛʀɪᴇs    ›  <code>{len(cash_reports)}</code>\n"
        f"⏱️  ᴜᴘᴛɪᴍᴇ            ›  <code>{up}</code>"
    )

    if mode_lines:
        text += f"\n<i>{DIV}</i>\n🔎  <b>ᴛᴏᴅᴀʏ ʙʏ ᴍᴏᴅᴇ</b>\n" + mode_lines

    if country_lines:
        text += f"<i>{DIV}</i>\n🌍  <b>ᴛᴏᴘ ᴄᴏᴜɴᴛʀɪᴇs</b>\n" + country_lines

    safe_reply_to(message, text)


@bot.message_handler(commands=["broadcast"])
def cmd_broadcast(message):
    if not is_admin(message.from_user.id):
        return
    if not message.reply_to_message:
        return safe_reply_to(message, "📢  ʀᴇᴘʟʏ ᴛᴏ ᴀ ᴍᴇssᴀɢᴇ ᴛᴏ ʙʀᴏᴀᴅᴄᴀsᴛ.")

    users = get_all_users()
    total = len(users)

    mu = InlineKeyboardMarkup()
    mu.add(
        InlineKeyboardButton(
            "✅  ʜᴀᴀɴ, sᴇɴᴅ ᴋᴀʀᴏ",
            callback_data=f"bc_confirm_{message.chat.id}_{message.reply_to_message.message_id}"
        ),
        InlineKeyboardButton("❌  ᴄᴀɴᴄᴇʟ", callback_data="bc_cancel")
    )
    safe_reply_to(message,
        "📢  <b>ʙʀᴏᴀᴅᴄᴀsᴛ ᴘʀᴇᴠɪᴇᴡ</b>\n"
        f"<i>{DIV}</i>\n"
        f"👥  ᴛᴏᴛᴀʟ ᴜsᴇʀs  ›  <code>{total}</code>\n\n"
        "ᴜᴘᴀʀ ᴡᴀʟᴀ ᴍᴇssᴀɢᴇ ʙʜᴇᴊᴀ ᴊᴀᴀᴇɢᴀ. ᴄᴏɴғɪʀᴍ ᴋᴀʀᴏ?",
        reply_markup=mu
    )


@bot.message_handler(commands=["makecode"])
def cmd_makecode(message):
    if not is_admin(message.from_user.id):
        return

    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        return safe_reply_to(message,
            "🎫  <b>ᴜsᴀɢᴇ:</b>  <code>/makecode [ᴀᴍᴛ] [ᴛɪᴍᴇ]</code>\n\n"
            "<b>ᴛɪᴍᴇ ᴇxᴀᴍᴘʟᴇs:</b>\n"
            "  <code>30s</code>   —  30 seconds\n"
            "  <code>10m</code>   —  10 minutes\n"
            "  <code>2h</code>    —  2 hours\n"
            "  <code>1d</code>    —  1 day\n"
            "  <code>1h30m</code> —  1 hour 30 min\n"
            "  <i>(no time = never expires)</i>"
        )

    amt = int(parts[1])
    expiry = None
    duration_secs = None
    expiry_text = "⏳  ᴇxᴘɪʀʏ   ›  <code>ɴᴇᴠᴇʀ</code>"

    if len(parts) >= 3:
        duration_secs = parse_duration(parts[2])
        if duration_secs is None:
            return safe_reply_to(message,
                "❌  <b>ɪɴᴠᴀʟɪᴅ ᴛɪᴍᴇ ғᴏʀᴍᴀᴛ</b>\n"
                "ᴜsᴇ: <code>30s</code>, <code>10m</code>, <code>2h</code>, <code>1d</code>"
            )
        expiry = int(time.time()) + duration_secs

        # Human-readable duration
        if duration_secs < 60:
            dur_str = f"{duration_secs}s"
        elif duration_secs < 3600:
            dur_str = f"{duration_secs // 60}m {duration_secs % 60}s"
        elif duration_secs < 86400:
            dur_str = f"{duration_secs // 3600}h {(duration_secs % 3600) // 60}m"
        else:
            dur_str = f"{duration_secs // 86400}d {(duration_secs % 86400) // 3600}h"

        expiry_text = f"⏳  ᴇxᴘɪʀʏ   ›  <code>{dur_str}</code>  ({fmt_ts(expiry)})"

    code = "NX-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=8))
    if create_code(code, amt, expiry):
        safe_reply_to(message,
            "╔══════════════════════════\n"
            "  🎫  <b>ᴄᴏᴅᴇ ɢᴇɴᴇʀᴀᴛᴇᴅ</b>\n"
            "╚══════════════════════════\n"
            f"🔑  ᴄᴏᴅᴇ   ›  <code>{code}</code>\n"
            f"💎  ᴠᴀʟᴜᴇ  ›  <code>{amt} ᴄʀᴇᴅɪᴛs</code>\n"
            f"{expiry_text}"
        )
    else:
        safe_reply_to(message, "❌  ᴄᴏᴅᴇ ɢᴇɴᴇʀᴀᴛɪᴏɴ ғᴀɪʟᴇᴅ.")


@bot.message_handler(commands=["usedcode"])
def cmd_usedcode(message):
    if not is_admin(message.from_user.id):
        return

    parts = message.text.split()
    if len(parts) < 2:
        return safe_reply_to(message, "❌  <code>/usedcode [CODE]</code>")

    code_text = parts[1].strip().upper()
    info = get_code_info(code_text)

    if not info:
        return safe_reply_to(message, "❌  ᴄᴏᴅᴇ ɴᴏᴛ ғᴏᴜɴᴅ.")

    used_status = "✅ ᴜsᴇᴅ" if info["used"] else "🟢 ᴀᴠᴀɪʟᴀʙʟᴇ"
    expiry_str  = fmt_expiry(info["expiry"])
    created_str = fmt_ts(info["created_at"])
    used_at_str = fmt_ts(info["used_at"]) if info["used_at"] else "—"

    user_part = "—"
    if info["used_by"]:
        uid_str   = f"<code>{info['used_by']}</code>"
        uname_str = f"@{html.escape(info['username'])}" if info["username"] else "N/A"
        fname_str = html.escape(info["first_name"] or "N/A")
        user_part = f"{uid_str}  {uname_str}  <i>{fname_str}</i>"

    safe_reply_to(message,
        "╔══════════════════════════\n"
        "  🔍  <b>ᴄᴏᴅᴇ ɪɴᴛᴇʟ</b>\n"
        "╚══════════════════════════\n"
        f"🔑  ᴄᴏᴅᴇ       ›  <code>{html.escape(code_text)}</code>\n"
        f"💎  ᴠᴀʟᴜᴇ      ›  <code>{info['value']} ᴄʀᴇᴅɪᴛs</code>\n"
        f"📡  sᴛᴀᴛᴜs     ›  {used_status}\n"
        f"⏳  ᴇxᴘɪʀʏ     ›  <code>{expiry_str}</code>\n"
        f"🕐  ᴄʀᴇᴀᴛᴇᴅ   ›  <code>{created_str}</code>\n"
        f"<i>{DIV}</i>\n"
        f"👤  ᴜsᴇᴅ ʙʏ    ›  {user_part}\n"
        f"🕑  ᴜsᴇᴅ ᴀᴛ    ›  <code>{used_at_str}</code>"
    )


@bot.message_handler(commands=["giveall"])
def cmd_giveall(message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        return safe_reply_to(message, "❌  /giveall [ᴀᴍᴛ]")
    amt = int(parts[1])
    mu  = InlineKeyboardMarkup()
    mu.add(
        InlineKeyboardButton(f"✅  ʜᴀᴀɴ, +{amt} sᴀʙᴋᴏ", callback_data=f"giveall_confirm_{amt}"),
        InlineKeyboardButton("❌  ᴄᴀɴᴄᴇʟ", callback_data="admin_cancel")
    )
    safe_reply_to(message,
        f"⚠️  <b>ᴄᴏɴғɪʀᴍ ɢɪᴠᴇᴀʟʟ</b>\n"
        f"<i>{DIV}</i>\n"
        f"sᴀʙ ᴜsᴇʀs ᴋᴏ <code>+{amt} ᴄʀᴇᴅɪᴛs</code> ᴅᴇɴᴇ ʜᴀɪɴ?",
        reply_markup=mu
    )


@bot.message_handler(commands=["addcredits"])
def cmd_addcredits(message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) < 3 or not parts[1].isdigit() or not parts[2].isdigit():
        return safe_reply_to(message, "❌  /addcredits [ᴜɪᴅ] [ᴀᴍᴛ]")
    target  = int(parts[1])
    amt     = int(parts[2])
    new_bal = add_credits_to_user(target, amt)
    if new_bal is not False and new_bal is not None:
        safe_reply_to(message,
            "✅  <b>ᴄʀᴇᴅɪᴛs ᴀᴅᴅᴇᴅ</b>\n"
            f"<i>{DIV}</i>\n"
            f"👤  ᴜsᴇʀ     ›  <code>{target}</code>\n"
            f"💰  ᴀᴅᴅᴇᴅ    ›  <code>+{amt}</code>\n"
            f"💎  ʙᴀʟᴀɴᴄᴇ  ›  <code>{new_bal}</code>"
        )
    else:
        safe_reply_to(message, "❌  ᴇʀʀᴏʀ ᴏʀ ᴜsᴇʀ ɴᴏᴛ ғᴏᴜɴᴅ.")


@bot.message_handler(commands=["userlist"])
def cmd_userlist(message):
    if not is_admin(message.from_user.id):
        return

    users = get_all_users_detail()
    if not users:
        return safe_reply_to(message, "📭  ɴᴏ ᴜsᴇʀs ʏᴇᴛ.")

    total  = len(users)
    banned = sum(1 for u in users if u[4] == 1)
    active = total - banned

    chunk_size = 25
    chunks = [users[i:i+chunk_size] for i in range(0, total, chunk_size)]

    for idx, chunk in enumerate(chunks):
        lines = []
        for u in chunk:
            row_uid, row_uname, row_fname, row_credits, row_banned, row_refs = u
            name_part  = html.escape(str(row_fname or "N/A"))
            uname_part = ("@" + html.escape(str(row_uname))) if row_uname else "—"
            icon       = "🚫" if row_banned else "🟢"
            entry = (
                f"{icon}  <code>{row_uid}</code>  {uname_part}\n"
                f"     👤  {name_part}\n"
                f"     💎  <code>{row_credits}</code>  ✦  <code>{row_refs} refs</code>"
            )
            lines.append(entry)

        header = ""
        if idx == 0:
            header = (
                "╔══════════════════════════\n"
                "  👥  <b>ᴜsᴇʀ ʟɪsᴛ</b>\n"
                "╚══════════════════════════\n"
                f"ᴛᴏᴛᴀʟ  ›  <code>{total}</code>  "
                f"🟢  <code>{active}</code>  "
                f"🚫  <code>{banned}</code>\n"
                f"<i>{DIV}</i>\n\n"
            )

        page_lbl = (
            f"\n\n<i>ᴘᴀɢᴇ  {idx + 1} / {len(chunks)}</i>"
        ) if len(chunks) > 1 else ""

        try:
            bot.send_message(
                message.chat.id,
                header + "\n\n".join(lines) + page_lbl,
                parse_mode="HTML"
            )
        except Exception as exc:
            logger.error("cmd_userlist chunk %d send error: %s", idx, exc)


@bot.message_handler(commands=["userinfo"])
def cmd_userinfo(message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        return safe_reply_to(message, "❌  /userinfo [ᴜɪᴅ]")

    u = get_user(int(parts[1]))
    if not u:
        return safe_reply_to(message, "❌  ᴜsᴇʀ ɴᴏᴛ ғᴏᴜɴᴅ.")

    uid, credits, daily_used, daily_reset, banned, refer_count = u
    status   = "🚫  ʙᴀɴɴᴇᴅ" if banned else "🟢  ᴀᴄᴛɪᴠᴇ"
    reset_in = fmt_reset(daily_reset)
    bar      = fmt_bar(daily_used, DAILY_LIMIT)

    safe_reply_to(message,
        "╔══════════════════════════\n"
        "  👤  <b>ᴜsᴇʀ ɪɴғᴏ</b>\n"
        "╚══════════════════════════\n"
        f"🆔  ɪᴅ          ›  <code>{uid}</code>\n"
        f"📡  sᴛᴀᴛᴜs     ›  {status}\n"
        f"💎  ᴄʀᴇᴅɪᴛs    ›  <code>{credits}</code>\n"
        f"📊  ᴅᴀɪʟʏ       ›  {bar}\n"
        f"⏱️  ʀᴇsᴇᴛ ɪɴ   ›  <code>{reset_in}</code>\n"
        f"✦   ʀᴇғᴇʀʀᴀʟs  ›  <code>{refer_count}</code>"
    )


@bot.message_handler(commands=["detail"])
def cmd_detail(message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        return safe_reply_to(message, "❌  /detail [ᴜɪᴅ]")

    target = int(parts[1])
    logs   = get_user_history(target, limit=30)
    if not logs:
        return safe_reply_to(message, f"📭  ɴᴏ ʜɪsᴛᴏʀʏ ғᴏʀ <code>{target}</code>.")

    text = (
        f"🕵️  <b>sᴇᴀʀᴄʜ ʜɪsᴛᴏʀʏ</b>  ›  <code>{target}</code>\n"
        f"<i>{DIV}</i>\n\n"
    )
    for i, (query, mode, ts) in enumerate(logs, 1):
        ts_str = ts.strftime("%d/%m/%y %H:%M") if ts else "?"
        text  += f"<code>{i:02d}</code>  {mode.upper():<8}  <code>{html.escape(str(query))}</code>  <i>{ts_str}</i>\n"
    text += f"\n<i>ᴛᴏᴛᴀʟ  ›  {len(logs)} ʟᴏɢs</i>"
    safe_reply_to(message, text)


@bot.message_handler(commands=["ban"])
def cmd_ban(message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        return safe_reply_to(message, "❌  /ban [ᴜɪᴅ]")
    target = parts[1]
    mu = InlineKeyboardMarkup()
    mu.add(
        InlineKeyboardButton("🚫  ʜᴀᴀɴ, ʙᴀɴ ᴋᴀʀᴏ", callback_data=f"ban_confirm_{target}"),
        InlineKeyboardButton("❌  ᴄᴀɴᴄᴇʟ", callback_data="admin_cancel")
    )
    safe_reply_to(message,
        f"⚠️  <b>ᴄᴏɴғɪʀᴍ ʙᴀɴ</b>\n"
        f"<i>{DIV}</i>\n"
        f"ᴜsᴇʀ <code>{target}</code> ᴋᴏ ʙᴀɴ ᴋᴀʀɴᴀ ʜᴀɪ?",
        reply_markup=mu
    )


@bot.message_handler(commands=["unban"])
def cmd_unban(message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        return safe_reply_to(message, "❌  /unban [ᴜɪᴅ]")
    unban_user(int(parts[1]))
    safe_reply_to(message, f"✅  <code>{parts[1]}</code>  ᴜɴʙᴀɴɴᴇᴅ.")


@bot.message_handler(commands=["lock"])
def cmd_lock(message):
    if not is_admin(message.from_user.id):
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return safe_reply_to(message, "❌  /lock [ǫᴜᴇʀʏ]")
    raw = args[1].strip()
    e164, _ = normalize_phone(raw)
    q = e164 if e164 else raw
    add_lock(q)
    safe_reply_to(message, f"🔒  ʟᴏᴄᴋᴇᴅ  ›  <code>{html.escape(q)}</code>")


@bot.message_handler(commands=["unlock"])
def cmd_unlock(message):
    if not is_admin(message.from_user.id):
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return safe_reply_to(message, "❌  /unlock [ǫᴜᴇʀʏ]")
    raw = args[1].strip()
    e164, _ = normalize_phone(raw)
    q = e164 if e164 else raw
    if remove_lock(q):
        safe_reply_to(message, f"🔓  ᴜɴʟᴏᴄᴋᴇᴅ  ›  <code>{html.escape(q)}</code>")
    else:
        safe_reply_to(message, "⚠️  ɴᴏᴛ ɪɴ ʟᴏᴄᴋ ʟɪsᴛ.")


@bot.message_handler(commands=["listlocked"])
def cmd_listlocked(message):
    if not is_admin(message.from_user.id):
        return
    locked = get_locked_list()
    if not locked:
        return safe_reply_to(message, "📭  ɴᴏ ʟᴏᴄᴋᴇᴅ ǫᴜᴇʀɪᴇs.")

    text = f"🔒  <b>ʟᴏᴄᴋᴇᴅ ǫᴜᴇʀɪᴇs  ›  {len(locked)}</b>\n<i>{DIV}</i>\n\n"
    for i, q in enumerate(locked, 1):
        text += f"<code>{i:02d}</code>  <code>{html.escape(q)}</code>\n"
        if len(text) > 3500:
            text += "\n<i>...ᴀɴᴅ ᴍᴏʀᴇ</i>"
            break
    safe_reply_to(message, text)


@bot.message_handler(commands=["shutdown"])
def cmd_shutdown(message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) < 2:
        return safe_reply_to(message, "❌  /shutdown [on/off]")
    if parts[1].lower() == "on":
        _shutdown_event.set()
        safe_reply_to(message,
            "🛑  <b>sʏsᴛᴇᴍ sʜᴜᴛᴅᴏᴡɴ ᴀᴄᴛɪᴠᴀᴛᴇᴅ.</b>\n"
            "<i>ᴜsᴇʀs ᴄᴀɴɴᴏᴛ ᴀᴄᴄᴇss ᴛʜᴇ ʙᴏᴛ.</i>"
        )
    else:
        _shutdown_event.clear()
        safe_reply_to(message,
            "✅  <b>sʏsᴛᴇᴍ ᴏɴʟɪɴᴇ.</b>\n"
            "<i>ʙᴏᴛ ɪs ʙᴀᴄᴋ ᴏɴʟɪɴᴇ.</i>"
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CALLBACKS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.callback_query_handler(func=lambda call: True)
def handle_cb(call):
    uid = call.from_user.id

    # ── Join verification ──
    if call.data == "check_join":
        if is_joined(uid):
            safe_answer_callback(call.id, "✅  ᴀᴄᴄᴇss ɢʀᴀɴᴛᴇᴅ!")
            try:
                bot.edit_message_text(
                    "✅  <b>ᴀᴄᴄᴇss ɢʀᴀɴᴛᴇᴅ!</b>\n"
                    f"<i>{DIV}</i>\n"
                    "ᴛʏᴘᴇ /start ᴛᴏ ʙᴇɢɪɴ.",
                    call.message.chat.id, call.message.message_id
                ,
                parse_mode="HTML")
            except Exception:
                pass
        else:
            safe_answer_callback(call.id,
                "❌  ᴘʟᴇᴀsᴇ ᴊᴏɪɴ ᴀʟʟ ᴄʜᴀɴɴᴇʟs ғɪʀsᴛ!",
                show_alert=True
            )

    # ── Refer ──
    elif call.data == "refer_now":
        u     = get_user(uid)
        count = u[5] if u else 0
        link  = f"https://t.me/{_BOT_USERNAME}?start={uid}"
        bot.send_message(call.message.chat.id,
            "✦  <b>ʀᴇғᴇʀʀᴀʟ ʟɪɴᴋ</b>\n"
            f"<i>{DIV}</i>\n"
            f"<code>{link}</code>\n\n"
            f"👥  ᴛᴏᴛᴀʟ  ›  <code>{count}</code>  ·  "
            "🎁  <b>+2 ᴄʀᴇᴅɪᴛs</b> ᴘᴇʀ ɪɴᴠɪᴛᴇ"
        )
        safe_answer_callback(call.id)

    # ── New search ──
    elif call.data == "new_search":
        safe_answer_callback(call.id)
        bot.send_message(call.message.chat.id,
            "🔍  <b>ɴᴇᴡ sᴇᴀʀᴄʜ</b>\n"
            f"<i>{DIV}</i>\n"
            "📞 /number  📧 /email  🪪 /aadhar\n"
            "💳 /pan  🚗 /vehicle  🌐 /ip"
        )

    # ── Profile inline ──
    elif call.data == "my_profile":
        safe_answer_callback(call.id)
        u = get_user(uid)
        if u:
            credits    = u[1]
            daily_used = u[2]
            daily_reset= u[3]
            refer_count= u[5]
            remaining  = max(0, DAILY_LIMIT - daily_used)
            bar        = fmt_bar(daily_used, DAILY_LIMIT)
            bot.send_message(call.message.chat.id,
                "◉  <b>ᴏᴘᴇʀᴀᴛɪᴠᴇ ᴘʀᴏғɪʟᴇ</b>\n"
                f"<i>{DIV}</i>\n"
                f"💎  ᴄʀᴇᴅɪᴛs   ›  <code>{credits}</code>\n"
                f"📊  {bar}\n"
                f"🆓  ʀᴇᴍᴀɪɴɪɴɢ  ›  <code>{remaining}</code>\n"
                f"⏱️  ʀᴇsᴇᴛ      ›  <code>{fmt_reset(daily_reset)}</code>\n"
                f"✦   ʀᴇғᴇʀʀᴀʟs  ›  <code>{refer_count}</code>",
                reply_markup=mk_buy()
            )

    # ── Pagination ──
    elif call.data.startswith("pg_"):
        parts = call.data.split("_", 2)
        if len(parts) != 3:
            return safe_answer_callback(call.id)

        _, qid, p_str = parts
        if qid not in cash_reports:
            return safe_answer_callback(
                call.id, "⚠️  sᴇssɪᴏɴ ᴇxᴘɪʀᴇᴅ. sᴇᴀʀᴄʜ ᴀɢᴀɪɴ.", show_alert=True
            )

        try:
            p = int(p_str)
        except ValueError:
            return safe_answer_callback(call.id)

        entry   = cash_reports[qid]
        results = entry["pages"] if isinstance(entry, dict) else entry
        total   = len(results)
        p       = p % total
        markup  = mk_search_done(qid, p, total)

        try:
            bot.edit_message_text(
                results[p] + WATERMARK,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                disable_web_page_preview=True
            ,
                parse_mode="HTML")
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
        if qid not in cash_reports:
            return safe_answer_callback(call.id, "⚠️  sᴇssɪᴏɴ ᴇxᴘɪʀᴇᴅ.", show_alert=True)
        try:
            p       = int(p_str)
            entry   = cash_reports[qid]
            results = entry["pages"] if isinstance(entry, dict) else entry
            plain   = re.sub(r'<[^>]+>', '', results[p % len(results)])
            bot.send_message(call.message.chat.id,
                f"📋  <b>ᴄᴏᴘʏ ᴋᴀʀᴇɴ:</b>\n\n<code>{html.escape(plain.strip())}</code>"
            )
            safe_answer_callback(call.id, "✅  ᴄᴏᴘɪᴇᴅ!")
        except Exception:
            safe_answer_callback(call.id)

    # ── Ban confirm ──
    elif call.data.startswith("ban_confirm_"):
        if not is_admin(uid):
            return safe_answer_callback(call.id)
        target = int(call.data.replace("ban_confirm_", "", 1))
        ban_user(target)
        try:
            bot.edit_message_text(
                f"🚫  <code>{target}</code>  ʙᴀɴ ʜᴏ ɢᴀʏᴀ.",
                call.message.chat.id, call.message.message_id
            ,
                parse_mode="HTML")
        except Exception:
            pass
        safe_answer_callback(call.id, "✅  ʙᴀɴ ᴅᴏɴᴇ")

    # ── Giveall confirm ──
    elif call.data.startswith("giveall_confirm_"):
        if not is_admin(uid):
            return safe_answer_callback(call.id)
        amt = int(call.data.replace("giveall_confirm_", "", 1))
        if give_all_credits(amt):
            try:
                bot.edit_message_text(
                    f"🎁  <b>ᴅᴏɴᴇ!</b>  sᴀʙᴋᴏ <code>+{amt} ᴄʀᴇᴅɪᴛs</code> ᴍɪʟ ɢᴀʏᴇ.",
                    call.message.chat.id, call.message.message_id
                ,
                parse_mode="HTML")
            except Exception:
                pass
        safe_answer_callback(call.id)

    # ── Broadcast confirm — runs in background thread ──
    elif call.data.startswith("bc_confirm_"):
        if not is_admin(uid):
            return safe_answer_callback(call.id)
        remainder    = call.data[len("bc_confirm_"):]
        src_chat_str, orig_mid_str = remainder.rsplit("_", 1)
        src_chat = int(src_chat_str)
        orig_mid = int(orig_mid_str)

        try:
            bot.edit_message_text(
                "📡  <b>ʙʀᴏᴀᴅᴄᴀsᴛɪɴɢ ɪɴ ʙᴀᴄᴋɢʀᴏᴜɴᴅ...</b>",
                call.message.chat.id, call.message.message_id
            ,
                parse_mode="HTML")
        except Exception:
            pass
        safe_answer_callback(call.id)

        # ← Non-blocking broadcast
        def _do_broadcast():
            users = get_all_users()
            s = f = 0
            for u in users:
                try:
                    bot.copy_message(u[0], src_chat, orig_mid)
                    s += 1
                    time.sleep(0.05)
                except Exception:
                    f += 1
            try:
                bot.send_message(
                    call.message.chat.id,
                    "✅  <b>ʙʀᴏᴀᴅᴄᴀsᴛ ᴄᴏᴍᴘʟᴇᴛᴇ</b>\n"
                    f"<i>{DIV}</i>\n"
                    f"📤  sᴇɴᴛ    ›  <code>{s}</code>\n"
                    f"❌  ғᴀɪʟᴇᴅ  ›  <code>{f}</code>"
                )
            except Exception:
                pass

        threading.Thread(target=_do_broadcast, daemon=True).start()

    # ── Cancel ──
    elif call.data in ("bc_cancel", "admin_cancel"):
        if not is_admin(uid):
            return safe_answer_callback(call.id)
        try:
            bot.edit_message_text(
                "❌  <b>ᴄᴀɴᴄᴇʟ ʜᴏ ɢᴀʏᴀ.</b>",
                call.message.chat.id, call.message.message_id
            ,
                parse_mode="HTML")
        except Exception:
            pass
        safe_answer_callback(call.id)

    else:
        safe_answer_callback(call.id)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  LAUNCH
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if __name__ == "__main__":
    logger.info("🚀 LaceraOsint starting...")
    while True:
        try:
            bot.infinity_polling(skip_pending=True, timeout=60, long_polling_timeout=30)
        except ApiTelegramException as exc:
            if exc.error_code == 409:
                logger.error("[POLL] 409 Conflict: another instance running. Waiting 15s...")
                time.sleep(15)
            elif exc.error_code == 429:
                retry = 10
                try:
                    retry = int(str(exc).split("retry after ")[-1])
                except Exception:
                    pass
                logger.warning("[POLL] 429 rate limit. Retry after %ds", retry)
                time.sleep(retry)
            else:
                logger.error("[POLL] ApiTelegramException: %s", exc)
                time.sleep(5)
        except Exception as exc:
            logger.error("[POLL] crash: %s", exc, exc_info=True)
            time.sleep(5)
