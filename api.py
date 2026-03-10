"""
api.py — LeakOSINT API wrapper for LaceraOSINT
Thread-safe rate limiting, full field mapping, country detection,
graceful error handling. Never raises to caller.
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

API_URL          = "https://leakosintapi.com/"
API_TOKEN        = os.getenv("API_TOKEN", "")
REQ_INTERVAL     = 0.4          # min seconds between requests
_rate_lock       = threading.Lock()
_last_request_ts = 0.0

# ══════════════════════════════════════════
#  COUNTRY DETECTION
# ══════════════════════════════════════════

# Phone prefix → (flag, country name)
COUNTRY_PHONE_MAP = {
    "91":  ("🇮🇳", "India"),
    "92":  ("🇵🇰", "Pakistan"),
    "1":   ("🇺🇸", "USA/Canada"),
    "7":   ("🇷🇺", "Russia"),
    "880": ("🇧🇩", "Bangladesh"),
    "44":  ("🇬🇧", "UK"),
    "86":  ("🇨🇳", "China"),
    "971": ("🇦🇪", "UAE"),
    "966": ("🇸🇦", "Saudi Arabia"),
    "49":  ("🇩🇪", "Germany"),
    "33":  ("🇫🇷", "France"),
    "81":  ("🇯🇵", "Japan"),
    "82":  ("🇰🇷", "South Korea"),
    "55":  ("🇧🇷", "Brazil"),
    "52":  ("🇲🇽", "Mexico"),
    "61":  ("🇦🇺", "Australia"),
    "234": ("🇳🇬", "Nigeria"),
    "20":  ("🇪🇬", "Egypt"),
    "27":  ("🇿🇦", "South Africa"),
    "62":  ("🇮🇩", "Indonesia"),
}

# TLD → (flag, country name)
COUNTRY_EMAIL_MAP = {
    ".in":    ("🇮🇳", "India"),
    ".pk":    ("🇵🇰", "Pakistan"),
    ".us":    ("🇺🇸", "USA"),
    ".ru":    ("🇷🇺", "Russia"),
    ".bd":    ("🇧🇩", "Bangladesh"),
    ".uk":    ("🇬🇧", "UK"),
    ".cn":    ("🇨🇳", "China"),
    ".ae":    ("🇦🇪", "UAE"),
    ".sa":    ("🇸🇦", "Saudi Arabia"),
    ".de":    ("🇩🇪", "Germany"),
    ".fr":    ("🇫🇷", "France"),
    ".jp":    ("🇯🇵", "Japan"),
    ".kr":    ("🇰🇷", "South Korea"),
    ".br":    ("🇧🇷", "Brazil"),
    ".au":    ("🇦🇺", "Australia"),
    ".ng":    ("🇳🇬", "Nigeria"),
    ".eg":    ("🇪🇬", "Egypt"),
    ".za":    ("🇿🇦", "South Africa"),
    ".id":    ("🇮🇩", "Indonesia"),
    ".mx":    ("🇲🇽", "Mexico"),
}

# IP ranges for rough geo (partial — just for display hints)
IP_COUNTRY_RANGES = {
    # Major blocks — approximate, for display only
    "103.": ("🇮🇳", "India"),
    "49.":  ("🇮🇳", "India"),
    "223.": ("🇨🇳", "China"),
    "185.": ("🇷🇺", "Russia"),
    "5.":   ("🇷🇺", "Russia"),
    "50.":  ("🇺🇸", "USA"),
    "52.":  ("🇺🇸", "USA"),
}


def detect_country(query: str, mode: str) -> tuple:
    """Returns (flag, country_name) or ('🌍', 'Unknown').
    query for number mode is E164 without + e.g. '919876543210'
    """
    try:
        if mode == "number":
            for prefix in sorted(COUNTRY_PHONE_MAP.keys(), key=len, reverse=True):
                if query.startswith(prefix):
                    return COUNTRY_PHONE_MAP[prefix]

        if mode == "email":
            q_lower = query.lower()
            for tld, info in COUNTRY_EMAIL_MAP.items():
                if q_lower.endswith(tld):
                    return info

        if mode == "ip":
            for prefix, info in IP_COUNTRY_RANGES.items():
                if query.startswith(prefix):
                    return info

    except Exception:
        pass
    return ("🌍", "Unknown")


# ══════════════════════════════════════════
#  USER-FACING MESSAGES
# ══════════════════════════════════════════

_ERR_GENERIC = (
    "✦  <b>sᴇʀᴠɪᴄᴇ ᴜɴᴀᴠᴀɪʟᴀʙʟᴇ</b>\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "😔  ᴋᴜᴄʜ ᴛᴇᴋɴɪᴄᴀʟ ᴅɪᴋᴋᴀᴛ ᴀᴀ ɢᴀʏɪ ʜᴀɪ.\n"
    "ᴛʜᴏᴅɪ ᴅᴇʀ ʙᴀᴀᴅ ᴅᴏʙᴀʀᴀ ᴋᴏsʜɪsʜ ᴋᴀʀᴇɴ. 🙏"
)
_ERR_TIMEOUT = (
    "⏳  <b>ʀᴇǫᴜᴇsᴛ ᴛɪᴍᴇᴅ ᴏᴜᴛ</b>\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "😔  sᴇʀᴠᴇʀ ᴀʙʜɪ ʙᴜsʏ ʜᴀɪ.\n"
    "ᴋᴜᴄʜ sᴇᴄᴏɴᴅs ʙᴀᴀᴅ ᴅᴏʙᴀʀᴀ ᴛʀʏ ᴋᴀʀᴇɴ. 🙏"
)
_ERR_NETWORK = (
    "🌐  <b>ɴᴇᴛᴡᴏʀᴋ ᴇʀʀᴏʀ</b>\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "😔  sᴇʀᴠᴇʀ sᴇ ᴄᴏɴɴᴇᴄᴛ ɴᴀʜɪ ʜᴏ ᴘᴀʏᴀ.\n"
    "ᴛʜᴏᴅɪ ᴅᴇʀ ʙᴀᴀᴅ ᴅᴏʙᴀʀᴀ ᴋᴏsʜɪsʜ ᴋᴀʀᴇɴ. 🙏"
)
_NO_RESULTS = "🔍  <b>ɴᴏ ʀᴇᴄᴏʀᴅs ғᴏᴜɴᴅ.</b>"

# ══════════════════════════════════════════
#  FIELD MAPPING
# ══════════════════════════════════════════

FIELD_MAP = [
    # Identity — specific first
    ("aadhaar",       "🪪 ᴀᴀᴅʜᴀᴀʀ"),
    ("aadhar",        "🪪 ᴀᴀᴅʜᴀᴀʀ"),
    ("adhar",         "🪪 ᴀᴀᴅʜᴀᴀʀ"),
    ("voter_id",      "🗳️ ᴠᴏᴛᴇʀ ɪᴅ"),
    ("voter",         "🗳️ ᴠᴏᴛᴇʀ ɪᴅ"),
    ("passport",      "🛂 ᴘᴀssᴘᴏʀᴛ"),
    ("pan_number",    "💳 ᴘᴀɴ"),
    ("pan_no",        "💳 ᴘᴀɴ"),
    ("pan",           "💳 ᴘᴀɴ"),
    ("driving",       "🚗 ᴅʟ ɴᴜᴍʙᴇʀ"),
    ("dl_no",         "🚗 ᴅʟ ɴᴜᴍʙᴇʀ"),
    ("doc_number",    "📄 ᴅᴏᴄ ɴᴜᴍʙᴇʀ"),
    ("document_no",   "📄 ᴅᴏᴄ ɴᴜᴍʙᴇʀ"),
    ("document",      "📄 ᴅᴏᴄᴜᴍᴇɴᴛ"),
    ("national_id",   "🪪 ɴᴀᴛɪᴏɴᴀʟ ɪᴅ"),
    ("nid",           "🪪 ɴɪᴅ"),
    ("cnic",          "🪪 ᴄɴɪᴄ"),
    # Names
    ("full_name",     "👤 ғᴜʟʟ ɴᴀᴍᴇ"),
    ("first_name",    "👤 ғɪʀsᴛ ɴᴀᴍᴇ"),
    ("last_name",     "👤 ʟᴀsᴛ ɴᴀᴍᴇ"),
    ("middle_name",   "👤 ᴍɪᴅᴅʟᴇ ɴᴀᴍᴇ"),
    ("father_name",   "👨 ғᴀᴛʜᴇʀ ɴᴀᴍᴇ"),
    ("father",        "👨 ғᴀᴛʜᴇʀ"),
    ("mother_name",   "👩 ᴍᴏᴛʜᴇʀ ɴᴀᴍᴇ"),
    ("mother",        "👩 ᴍᴏᴛʜᴇʀ"),
    ("spouse",        "💑 sᴘᴏᴜsᴇ"),
    ("husband",       "💑 ʜᴜsʙᴀɴᴅ"),
    ("wife",          "💑 ᴡɪғᴇ"),
    ("name",          "👤 ɴᴀᴍᴇ"),
    # Contact
    ("mobile",        "📱 ᴍᴏʙɪʟᴇ"),
    ("telephone",     "📞 ᴛᴇʟᴇᴘʜᴏɴᴇ"),
    ("phone",         "📞 ᴘʜᴏɴᴇ"),
    ("email",         "📧 ᴇᴍᴀɪʟ"),
    ("mail",          "📧 ᴍᴀɪʟ"),
    # Auth — password AFTER passport
    ("password",      "🔑 ᴘᴀssᴡᴏʀᴅ"),
    ("hash",          "🔒 ʜᴀsʜ"),
    ("salt",          "🧂 sᴀʟᴛ"),
    ("login",         "🆔 ʟᴏɢɪɴ"),
    ("username",      "🆔 ᴜsᴇʀɴᴀᴍᴇ"),
    ("user",          "🆔 ᴜsᴇʀ"),
    # Address
    ("pincode",       "📮 ᴘɪɴᴄᴏᴅᴇ"),
    ("pin_code",      "📮 ᴘɪɴᴄᴏᴅᴇ"),
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
    ("dob",           "📅 ᴅ.ᴏ.ʙ"),
    ("birth",         "📅 ᴅ.ᴏ.ʙ"),
    ("age",           "🎂 ᴀɢᴇ"),
    ("gender",        "⚧️ ɢᴇɴᴅᴇʀ"),
    ("sex",           "⚧️ ɢᴇɴᴅᴇʀ"),
    ("nationality",   "🌍 ɴᴀᴛɪᴏɴᴀʟɪᴛʏ"),
    ("religion",      "✝️ ʀᴇʟɪɢɪᴏɴ"),
    # Network
    ("ip_address",    "🌐 ɪᴘ ᴀᴅᴅʀᴇss"),
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
    ("facebook",      "📘 ғᴀᴄᴇʙᴏᴏᴋ"),
    ("instagram",     "📸 ɪɴsᴛᴀɢʀᴀᴍ"),
    ("twitter",       "🐦 ᴛᴡɪᴛᴛᴇʀ"),
    ("linkedin",      "💼 ʟɪɴᴋᴇᴅɪɴ"),
    ("skype",         "💬 sᴋʏᴘᴇ"),
    ("whatsapp",      "💬 ᴡʜᴀᴛsᴀᴘᴘ"),
    ("telegram",      "✈️ ᴛᴇʟᴇɢʀᴀᴍ"),
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
        return ", ".join(parts)
    if isinstance(val, dict):
        return ""
    return str(val).strip()


def _get_label(key: str):
    k = key.lower().strip()
    # Exact match
    for ref, label in FIELD_MAP:
        if k == ref:
            return label
    # Starts-with
    for ref, label in FIELD_MAP:
        if k.startswith(ref + "_") or k.startswith(ref):
            return label
    # Contains (only 4+ char refs)
    for ref, label in FIELD_MAP:
        if len(ref) >= 4 and ref in k:
            return label
    return None


def _format_query(query: str, mode: str) -> str:
    q = str(query).strip()
    if not q:
        return ""
    if mode == "number":
        # query should be E164 (with CC) from normalize_phone
        # Safety net: bare 10-digit without CC — add 91 (India) as fallback
        if q.isdigit() and len(q) == 10:
            return f"91{q}"
        return q
    if " " in q and not q.startswith('"'):
        return f'"{q}"'
    return q


def _page_header(db_name: str, flag: str = "", country: str = "",
                 info_leak: str = "", cont: bool = False) -> str:
    safe = html.escape(str(db_name).upper())
    loc  = f"  {flag} <i>{html.escape(country)}</i>\n" if flag and not cont else ""
    hdr  = "╔══════════════════════════\n"
    if cont:
        hdr += f"  🗄️  <b>{safe}</b>  <i>(ᴄᴏɴᴛ.)</i>\n"
    else:
        hdr += f"  🗄️  <b>{safe}</b>\n"
        hdr += loc
        if info_leak and str(info_leak).strip():
            snippet = html.escape(str(info_leak).strip()[:100])
            if len(str(info_leak)) > 100:
                snippet += "..."
            hdr += f"  📌 <i>{snippet}</i>\n"
    hdr += "╚══════════════════════════\n\n"
    return hdr


# ══════════════════════════════════════════
#  MAIN LOOKUP
# ══════════════════════════════════════════

def perform_lookup(query: str, mode: str = "") -> tuple:
    """
    Call LeakOSINT API.
    Returns (pages: list[str], country_name: str).
    Never raises.
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
        wait = REQ_INTERVAL - (time.time() - _last_request_ts)
        if wait > 0:
            time.sleep(wait)
        _last_request_ts = time.time()

    logger.info("[API] query='%s' mode=%s country=%s", q_str, mode, country_name)

    payload = {
        "token":   API_TOKEN,
        "request": q_str,
        "limit":   150,
        "lang":    "en",
    }

    try:
        resp = requests.post(
            API_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=55,
        )
    except requests.exceptions.Timeout:
        logger.warning("[API] timeout for query '%s'", q_str)
        return [_ERR_TIMEOUT], country_name
    except requests.exceptions.SSLError as exc:
        logger.error("[API] SSL error: %s", exc)
        return [_ERR_NETWORK], country_name
    except requests.exceptions.ConnectionError as exc:
        logger.error("[API] connection error: %s", exc)
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

    logger.info("[API] HTTP %s", resp.status_code)

    if not (200 <= resp.status_code < 300):
        logger.error("[API] bad status: %s", resp.status_code)
        return [_ERR_GENERIC], country_name

    if not resp.text or not resp.text.strip():
        return [_NO_RESULTS], country_name

    try:
        data = resp.json()
    except Exception as exc:
        logger.error("[API] JSON parse error: %s", exc)
        return [_ERR_GENERIC], country_name

    if not data or not isinstance(data, dict):
        return [_NO_RESULTS], country_name

    # API-level error
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

            record_text = "\n".join(record_lines) + "\n  ┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n\n"
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
        pages[-1] += "\n\n<i>⚠️ ᴛᴏᴏ ᴍᴀɴʏ ʀᴇsᴜʟᴛs — sʜᴏᴡɪɴɢ ᴛᴏᴘ 50 ᴘᴀɢᴇs.</i>"

    return (pages if pages else [_NO_RESULTS]), country_name
