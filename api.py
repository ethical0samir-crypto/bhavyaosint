"""
api.py — LeakOSINT API wrapper for LaceraOSINT
Thread-safe rate limiting, full field mapping, country detection,
graceful error handling. Never raises to caller.
Only supports: India (91), Pakistan (92), Bangladesh (880)
"""

import os
import re
import time
import html
import logging
import threading
import requests

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════

API_URL      = os.getenv("LEAKOSINT_URL", "https://leakosintapi.com/").rstrip("/") + "/"
API_TOKEN    = os.getenv("API_TOKEN", "").strip()
REQ_INTERVAL = float(os.getenv("API_RATE_INTERVAL", "0.4"))
_API_CONNECT_TIMEOUT = float(os.getenv("API_CONNECT_TIMEOUT", "10"))
_API_READ_TIMEOUT    = float(os.getenv("API_READ_TIMEOUT",    "45"))

_rate_lock       = threading.Lock()
_last_request_ts = 0.0

if not API_TOKEN:
    logger.warning("API_TOKEN not set — all lookups will fail")
elif len(API_TOKEN) < 8:
    logger.warning("API_TOKEN looks too short — check env")

_HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "LaceraOSINT-Bot/2.0",
}

_MAX_RESPONSE_BYTES = 10 * 1024 * 1024

# ══════════════════════════════════════════
#  COUNTRY DETECTION — 3 countries only
# ══════════════════════════════════════════

# Phone prefix → (flag, country name) — longest match first
COUNTRY_PHONE_MAP: dict[str, tuple[str, str]] = {
    "880": ("🇧🇩", "Bangladesh"),
    "91":  ("🇮🇳", "India"),
    "92":  ("🇵🇰", "Pakistan"),
}

_PHONE_PREFIXES_SORTED = sorted(COUNTRY_PHONE_MAP.keys(), key=len, reverse=True)

# Email TLD → (flag, country name)
COUNTRY_EMAIL_MAP: dict[str, tuple[str, str]] = {
    ".in": ("🇮🇳", "India"),
    ".pk": ("🇵🇰", "Pakistan"),
    ".bd": ("🇧🇩", "Bangladesh"),
}


def detect_country(query: str, mode: str) -> tuple[str, str]:
    """Returns (flag, country_name) or ('🌍', 'Unknown')."""
    try:
        if mode == "number":
            for prefix in _PHONE_PREFIXES_SORTED:
                if query.startswith(prefix):
                    return COUNTRY_PHONE_MAP[prefix]

        elif mode == "email":
            q_lower = query.lower()
            for tld in sorted(COUNTRY_EMAIL_MAP.keys(), key=len, reverse=True):
                if q_lower.endswith(tld):
                    return COUNTRY_EMAIL_MAP[tld]

    except Exception:
        pass
    return ("🌍", "Unknown")


# ══════════════════════════════════════════
#  USER-FACING ERROR MESSAGES
# ══════════════════════════════════════════

_ERR_GENERIC = (
    "✦  <b>sᴇʀᴠɪᴄᴇ ᴜɴᴀᴠᴀɪʟᴀʙʟᴇ</b>\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "😔  ᴋᴜᴄʜ ᴛᴇᴋɴɪᴋᴀʟ ᴅɪᴋᴋᴀᴛ ʜᴀɪ.\n"
    "ᴛʜᴏᴅɪ ᴅᴇʀ ʙᴀᴀᴅ ᴅᴏʙᴀʀᴀ ᴋᴀʀᴏ. 🙏"
)
_ERR_TIMEOUT = (
    "⏳  <b>ᴛɪᴍᴇ ᴏᴜᴛ</b>\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "😔  sᴇʀᴠᴇʀ ᴀʙʜɪ ʙᴜsʏ ʜᴀɪ.\n"
    "ᴛʜᴏᴅɪ ᴅᴇʀ ʙᴀᴀᴅ ᴅᴏʙᴀʀᴀ ᴋᴀʀᴏ. 🙏"
)
_ERR_NETWORK = (
    "🌐  <b>ɴᴇᴛᴡᴏʀᴋ ᴇʀʀᴏʀ</b>\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "😔  sᴇʀᴠᴇʀ ᴄᴏɴɴᴇᴄᴛ ɴᴀʜɪ ʜᴜᴀ.\n"
    "ᴛʜᴏᴅɪ ᴅᴇʀ ʙᴀᴀᴅ ᴅᴏʙᴀʀᴀ ᴋᴀʀᴏ. 🙏"
)
_NO_RESULTS = "🔍  <b>ᴋᴏɪ ʀᴇᴄᴏʀᴅ ɴᴀʜɪ ᴍɪʟᴀ.</b>"

# ══════════════════════════════════════════
#  FIELD MAPPING
# ══════════════════════════════════════════

FIELD_MAP = [
    # Identity
    ("aadhaar",       "🪪 ᴀᴀᴅʜᴀᴀʀ"),
    ("aadhar",        "🪪 ᴀᴀᴅʜᴀᴀʀ"),
    ("adhar",         "🪪 ᴀᴀᴅʜᴀᴀʀ"),
    ("voter_id",      "🗳️ ᴠᴏᴛᴇʀ ɪᴅ"),
    ("voter",         "🗳️ ᴠᴏᴛᴇʀ ɪᴅ"),
    ("passport",      "🛂 ᴘᴀssᴘᴏʀᴛ"),
    ("pan_number",    "💳 ᴘᴀɴ"),
    ("pan_no",        "💳 ᴘᴀɴ"),
    ("pan",           "💳 ᴘᴀɴ"),
    ("driving",       "🚗 ᴅʟ"),
    ("dl_no",         "🚗 ᴅʟ"),
    ("doc_number",    "📄 ᴅᴏᴄ ɴᴏ"),
    ("document_no",   "📄 ᴅᴏᴄ ɴᴏ"),
    ("document",      "📄 ᴅᴏᴄ"),
    ("national_id",   "🪪 ɴᴀᴛɪᴏɴᴀʟ ɪᴅ"),
    ("nid",           "🪪 ɴɪᴅ"),
    ("cnic",          "🪪 ᴄɴɪᴄ"),
    # Names
    ("full_name",     "👤 ɴᴀᴍᴇ"),
    ("first_name",    "👤 ғɪʀsᴛ"),
    ("last_name",     "👤 ʟᴀsᴛ"),
    ("middle_name",   "👤 ᴍɪᴅᴅʟᴇ"),
    ("father_name",   "👨 ғᴀᴛʜᴇʀ"),
    ("father",        "👨 ғᴀᴛʜᴇʀ"),
    ("mother_name",   "👩 ᴍᴏᴛʜᴇʀ"),
    ("mother",        "👩 ᴍᴏᴛʜᴇʀ"),
    ("spouse",        "💑 sᴘᴏᴜsᴇ"),
    ("husband",       "💑 ʜᴜsʙᴀɴᴅ"),
    ("wife",          "💑 ᴡɪғᴇ"),
    ("name",          "👤 ɴᴀᴍᴇ"),
    # Contact
    ("mobile",        "📱 ᴍᴏʙɪʟᴇ"),
    ("telephone",     "📞 ᴛᴇʟ"),
    ("phone",         "📞 ᴘʜᴏɴᴇ"),
    ("email",         "📧 ᴇᴍᴀɪʟ"),
    ("mail",          "📧 ᴍᴀɪʟ"),
    # Auth
    ("password",      "🔑 ᴘᴀssᴡᴏʀᴅ"),
    ("hash",          "🔒 ʜᴀsʜ"),
    ("salt",          "🧂 sᴀʟᴛ"),
    ("login",         "🆔 ʟᴏɢɪɴ"),
    ("username",      "🆔 ᴜsᴇʀɴᴀᴍᴇ"),
    ("user",          "🆔 ᴜsᴇʀ"),
    # Address
    ("pincode",       "📮 ᴘɪɴ"),
    ("pin_code",      "📮 ᴘɪɴ"),
    ("postal",        "📮 ᴘᴏsᴛᴀʟ"),
    ("zip",           "📮 ᴢɪᴘ"),
    ("address",       "🏠 ᴀᴅᴅʀᴇss"),
    ("city",          "🏙️ ᴄɪᴛʏ"),
    ("district",      "🏙️ ᴅɪsᴛʀɪᴄᴛ"),
    ("state",         "🏛️ sᴛᴀᴛᴇ"),
    ("country",       "🌍 ᴄᴏᴜɴᴛʀʏ"),
    ("region",        "🗺️ ʀᴇɢɪᴏɴ"),
    ("province",      "🗺️ ᴘʀᴏᴠɪɴᴄᴇ"),
    # Personal
    ("dob",           "📅 ᴅᴏʙ"),
    ("birth",         "📅 ᴅᴏʙ"),
    ("age",           "🎂 ᴀɢᴇ"),
    ("gender",        "⚧️ ɢᴇɴᴅᴇʀ"),
    ("sex",           "⚧️ ɢᴇɴᴅᴇʀ"),
    ("nationality",   "🌍 ɴᴀᴛɪᴏɴᴀʟɪᴛʏ"),
    ("religion",      "✝️ ʀᴇʟɪɢɪᴏɴ"),
    # Network
    ("ip_address",    "🌐 ɪᴘ"),
    ("ip",            "🌐 ɪᴘ"),
    ("mac",           "💻 ᴍᴀᴄ"),
    ("isp",           "📡 ɪsᴘ"),
    # Work
    ("company",       "💼 ᴄᴏᴍᴘᴀɴʏ"),
    ("employer",      "💼 ᴇᴍᴘʟᴏʏᴇʀ"),
    ("occupation",    "💼 ᴏᴄᴄᴜᴘᴀᴛɪᴏɴ"),
    ("work",          "💼 ᴡᴏʀᴋ"),
    ("job",           "💼 ᴊᴏʙ"),
    # Social
    ("facebook",      "📘 ғʙ"),
    ("instagram",     "📸 ɪɢ"),
    ("twitter",       "🐦 ᴛᴡɪᴛᴛᴇʀ"),
    ("linkedin",      "💼 ʟɪɴᴋᴇᴅɪɴ"),
    ("skype",         "💬 sᴋʏᴘᴇ"),
    ("whatsapp",      "💬 ᴡᴀ"),
    ("telegram",      "✈️ ᴛɢ"),
    ("vk",            "🌐 ᴠᴋ"),
    # Finance
    ("account",       "🏦 ᴀᴄᴄᴏᴜɴᴛ"),
    ("bank",          "🏦 ʙᴀɴᴋ"),
    ("ifsc",          "🏦 ɪғsᴄ"),
    ("upi",           "💸 ᴜᴘɪ"),
    ("income",        "💰 ɪɴᴄᴏᴍᴇ"),
    ("card",          "💳 ᴄᴀʀᴅ"),
    # Vehicle
    ("vehicle",       "🚗 ᴠᴇʜɪᴄʟᴇ"),
    ("plate",         "🚗 ᴘʟᴀᴛᴇ"),
    ("chassis",       "⚙️ ᴄʜᴀssɪs"),
    ("engine",        "⚙️ ᴇɴɢɪɴᴇ"),
    ("rto",           "🏛️ ʀᴛᴏ"),
]

_FM_EXACT: dict   = {ref: label for ref, label in FIELD_MAP}
_FM_CONTAINS: list = [(ref, label) for ref, label in FIELD_MAP if len(ref) >= 4]

_IGNORE_KEYS = {
    "image", "photo", "avatar", "icon", "thumbnail",
    "internal_id", "timestamp", "updated_at", "created_at",
    "modified_at", "deleted_at", "v_id", "object_id",
    "record_id", "_id", "id", "url", "link", "href", "src",
}

_GARBAGE = {
    "", "none", "null", "undefined", "n/a", "unknown",
    "[]", "{}", "0", "-", "na", "false", "true", "nil", "nan",
    "not available", "not provided", "n.a.", "na.", ".",
}

_MIN_KEY_LEN = 2
_QUERY_STRIP_RE = re.compile(r'[\x00-\x1f\x7f]')


# ══════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════

def _is_clean(val) -> bool:
    if val is None:
        return False
    if isinstance(val, (list, dict, bool)):
        return False
    s = str(val).strip()
    return s.lower() not in _GARBAGE and len(s) >= 1


def _flatten(val) -> str:
    if val is None:
        return ""
    if isinstance(val, list):
        parts = [
            str(v).strip() for v in val
            if v is not None and str(v).strip().lower() not in _GARBAGE
        ]
        return ", ".join(parts[:20])
    if isinstance(val, dict):
        return ""
    return str(val).strip()


def _get_label(key: str):
    """O(1) exact → prefix → contains fallback."""
    k = key.lower().strip()
    if k in _FM_EXACT:
        return _FM_EXACT[k]
    for ref, label in FIELD_MAP:
        if k.startswith(ref + "_") or (k != ref and k.startswith(ref)):
            return label
    for ref, label in _FM_CONTAINS:
        if ref in k:
            return label
    return None


def _format_query(query: str, mode: str) -> str:
    """
    Clean query for API. For number mode: digits only (no +).
    API expects: 919876543210 (not +919876543210)
    """
    q = str(query).strip()
    if not q:
        return ""
    q = _QUERY_STRIP_RE.sub("", q)
    # query already normalized to pure digits in bot.py for number mode
    return q


def _page_header(db_name: str, flag: str = "", country: str = "",
                 info_leak: str = "", cont: bool = False) -> str:
    safe = html.escape(str(db_name).upper())
    loc  = f"  {flag} <i>{html.escape(country)}</i>\n" if flag and not cont else ""
    hdr  = "┌─────────────────────────┐\n"
    if cont:
        hdr += f"  🗄️  <b>{safe}</b>  <i>(cont.)</i>\n"
    else:
        hdr += f"  🗄️  <b>{safe}</b>\n"
        hdr += loc
        if info_leak and str(info_leak).strip():
            snippet = html.escape(str(info_leak).strip()[:100])
            if len(str(info_leak)) > 100:
                snippet += "..."
            hdr += f"  📌 <i>{snippet}</i>\n"
    hdr += "└─────────────────────────┘\n\n"
    return hdr


# ══════════════════════════════════════════
#  MAIN LOOKUP
# ══════════════════════════════════════════

def perform_lookup(query: str, mode: str = "") -> tuple:
    """
    Call LeakOSINT API.
    Returns (pages: list[str], country_name: str).
    Never raises.
    query for number mode: pure digits e.g. 919876543210
    """
    global _last_request_ts

    flag, country_name = detect_country(query, mode)

    if not query or not str(query).strip():
        return [_NO_RESULTS], country_name

    if not API_TOKEN:
        logger.error("API_TOKEN not set")
        return [_ERR_GENERIC], country_name

    q_str = _format_query(query, mode)
    if not q_str:
        return [_NO_RESULTS], country_name

    # Thread-safe rate limiting
    with _rate_lock:
        now = time.time()
        wait = REQ_INTERVAL - (now - _last_request_ts)
        _last_request_ts = now + max(wait, 0)
    if wait > 0:
        time.sleep(wait)

    logger.info("[API] query='%s' mode=%s country=%s", q_str, mode, country_name)

    payload = {
        "token":   API_TOKEN,
        "request": q_str,
        "limit":   150,
        "lang":    "en",
    }

    resp = None
    for _attempt in range(3):
        try:
            resp = requests.post(
                API_URL,
                json=payload,
                headers=_HEADERS,
                timeout=(_API_CONNECT_TIMEOUT, _API_READ_TIMEOUT),
            )
        except requests.exceptions.Timeout:
            logger.warning("[API] timeout attempt=%d query='%s'", _attempt + 1, q_str)
            if _attempt < 2:
                time.sleep(2 ** _attempt)
                continue
            return [_ERR_TIMEOUT], country_name
        except requests.exceptions.SSLError as exc:
            logger.error("[API] SSL error: %s", exc)
            return [_ERR_NETWORK], country_name
        except requests.exceptions.ConnectionError as exc:
            logger.error("[API] connection error attempt=%d: %s", _attempt + 1, exc)
            if _attempt < 2:
                time.sleep(2 ** _attempt)
                continue
            return [_ERR_NETWORK], country_name
        except requests.exceptions.TooManyRedirects:
            logger.error("[API] too many redirects")
            return [_ERR_NETWORK], country_name
        except MemoryError:
            logger.critical("[API] memory error")
            return [_ERR_GENERIC], country_name
        except Exception as exc:
            logger.error("[API] unexpected: %s", exc)
            return [_ERR_GENERIC], country_name

        logger.info("[API] HTTP %s attempt=%d", resp.status_code, _attempt + 1)

        if resp.status_code in (502, 503, 504):
            logger.warning("[API] %s gateway error, retry %d/3", resp.status_code, _attempt + 1)
            if _attempt < 2:
                time.sleep(2 ** _attempt)
                continue
            return [_ERR_GENERIC], country_name

        if not (200 <= resp.status_code < 300):
            logger.error("[API] bad status: %s body=%s", resp.status_code, resp.text[:200])
            return [_ERR_GENERIC], country_name

        break

    if resp is None:
        return [_ERR_GENERIC], country_name

    content_len = len(resp.content)
    if content_len > _MAX_RESPONSE_BYTES:
        logger.error("[API] response too large: %d bytes", content_len)
        return [_ERR_GENERIC], country_name

    if not resp.text or not resp.text.strip():
        return [_NO_RESULTS], country_name

    ct = resp.headers.get("Content-Type", "")
    if "json" not in ct.lower() and resp.text.strip().startswith("<"):
        logger.error("[API] got HTML instead of JSON (HTTP %s)", resp.status_code)
        return [_ERR_GENERIC], country_name

    try:
        data = resp.json()
    except Exception as exc:
        logger.error("[API] JSON parse error: %s  body_start=%s", exc, resp.text[:100])
        return [_ERR_GENERIC], country_name

    if not data or not isinstance(data, dict):
        return [_NO_RESULTS], country_name

    if "Error code" in data:
        code = str(data.get("Error code", ""))
        msg  = str(data.get("Error message", ""))
        logger.warning("[API] error code=%s msg=%s", code, msg)
        if code in ("104", "105", "106"):
            return [_NO_RESULTS], country_name
        return [_ERR_GENERIC], country_name

    db_list = data.get("List")
    if not db_list or not isinstance(db_list, dict):
        return [_NO_RESULTS], country_name

    pages = []

    for db_name, db_content in db_list.items():
        if not db_name:
            continue
        db_name_s = str(db_name).strip()
        if db_name_s.lower() in ("", "no results found", "no results"):
            continue
        if not isinstance(db_content, dict):
            continue

        info_leak = str(db_content.get("InfoLeak") or "").strip()

        records = None
        for rkey in ("Data", "data", "Records", "records", "Results", "results"):
            v = db_content.get(rkey)
            if isinstance(v, list) and v:
                records = v
                break

        if not records:
            continue

        logger.debug("[API] db='%s' records=%d", db_name_s, len(records))

        page     = _page_header(db_name_s, flag, country_name, info_leak)
        has_data = False

        for report in records:
            if not isinstance(report, dict) or not report:
                continue

            record_lines = []
            seen_labels  = set()

            for raw_key, val in report.items():
                if raw_key is None:
                    continue
                k = str(raw_key).lower().strip()
                if len(k) < _MIN_KEY_LEN:
                    continue
                if k in _IGNORE_KEYS:
                    continue
                if any(ig in k for ig in ("image", "avatar", "photo", "icon", "url", "link")):
                    continue

                flat = _flatten(val)
                if not _is_clean(flat):
                    continue

                safe_val = html.escape(flat)
                if len(safe_val) > 250:
                    safe_val = safe_val[:247] + "..."

                label = _get_label(k)
                if label:
                    if label in seen_labels:
                        clean_key = html.escape(str(raw_key).replace("_", " ").title())
                        record_lines.append(f"  ▪ {clean_key}  <code>{safe_val}</code>")
                    else:
                        seen_labels.add(label)
                        record_lines.append(f"  {label}  <code>{safe_val}</code>")
                else:
                    clean_key = html.escape(str(raw_key).replace("_", " ").title())
                    record_lines.append(f"  ▪ {clean_key}  <code>{safe_val}</code>")

            if not record_lines:
                continue

            record_text = "\n".join(record_lines) + "\n  ┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n\n"
            has_data = True

            if len(page) + len(record_text) > 3600:
                if page.strip():
                    pages.append(page.rstrip() + "\n\n<i>  ↳ ᴄᴏɴᴛɪɴᴜᴇᴅ...</i>")
                page = _page_header(db_name_s, cont=True)

            page += record_text

        if has_data and page.strip():
            pages.append(page.rstrip())

    logger.info("[API] built %d pages", len(pages))

    if len(pages) > 50:
        pages = pages[:50]
        pages[-1] += "\n\n<i>⚠️ ᴛᴏᴘ 50 ᴘᴀɢᴇs sʜᴏᴡ ʜᴏ ʀᴀʜɪ.</i>"

    return (pages if pages else [_NO_RESULTS]), country_name
