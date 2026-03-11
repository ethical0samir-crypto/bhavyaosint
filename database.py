"""
database.py — Thread-safe PostgreSQL layer for LaceraOSINT
All queries parameterised — zero SQL injection surface.
"""

import os
import time
import logging
from psycopg2 import pool as pg_pool

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════
#  CONNECTION POOL
# ══════════════════════════════════════════

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if not DATABASE_URL:
    logger.critical("DATABASE_URL env not set — DB will be unavailable")

# DB-14 FIX: sslmode env-configurable (local dev can set SSL_MODE=disable)
_SSL_MODE = os.getenv("SSL_MODE", "require")

# DB-13 FIX: pool size env-configurable
_DB_MIN_CONN = int(os.getenv("DB_MIN_CONN", "2"))
_DB_MAX_CONN = int(os.getenv("DB_MAX_CONN", "20"))

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

try:
    db_pool = pg_pool.ThreadedConnectionPool(
        minconn=_DB_MIN_CONN,
        maxconn=_DB_MAX_CONN,
        dsn=DATABASE_URL,
        sslmode=_SSL_MODE,
        connect_timeout=10,
    ) if DATABASE_URL else None
    if db_pool:
        logger.info("✅ DB pool created — min=%d max=%d ssl=%s", _DB_MIN_CONN, _DB_MAX_CONN, _SSL_MODE)
except Exception as exc:
    logger.critical("❌ DB pool failed: %s", exc)
    db_pool = None  # type: ignore

# DB-02 FIX: ping only on closed/stale — track last good conn via set
_healthy_conns: set = set()
_healthy_lock = __import__("threading").Lock()


def get_conn():
    if not db_pool:
        logger.error("get_conn: pool is None")
        return None
    try:
        conn = db_pool.getconn()
        if conn is None:
            return None

        # Fast path — already known healthy
        with _healthy_lock:
            if id(conn) in _healthy_conns and not conn.closed:
                return conn

        # Check closed flag
        if conn.closed:
            logger.warning("get_conn: closed connection, discarding")
            try:
                db_pool.putconn(conn, close=True)
            except Exception:
                pass
            return db_pool.getconn()

        # Ping only unknown/returning connections
        try:
            _cur = conn.cursor()
            _cur.execute("SELECT 1")
            _cur.close()
            with _healthy_lock:
                _healthy_conns.add(id(conn))
        except Exception:
            logger.warning("get_conn: ping failed, discarding stale connection")
            with _healthy_lock:
                _healthy_conns.discard(id(conn))
            try:
                db_pool.putconn(conn, close=True)
            except Exception:
                pass
            try:
                conn = db_pool.getconn()
            except Exception as exc:
                logger.error("get_conn: failed to get fresh conn after discard: %s", exc)
                return None

        return conn
    except Exception as exc:
        logger.error("get_conn error: %s", exc)
        return None


def release(conn, error: bool = False):
    """Return connection to pool. Rollback if error=True or uncommitted state."""
    if not (db_pool and conn):
        return
    with _healthy_lock:
        if error:
            _healthy_conns.discard(id(conn))
    try:
        # DB-03 FIX: rollback if error to clear dirty state
        if error or conn.status == 2:  # status 2 = INTRANS_INERROR
            try:
                conn.rollback()
            except Exception:
                pass
            with _healthy_lock:
                _healthy_conns.discard(id(conn))
        db_pool.putconn(conn)
    except Exception as exc:
        logger.error("release error: %s", exc)


# ══════════════════════════════════════════
#  SCHEMA SETUP
# ══════════════════════════════════════════

def setup_db():
    conn = get_conn()
    if not conn:
        return
    cur = None
    try:
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id          BIGINT  PRIMARY KEY,
                credits     INTEGER DEFAULT 0,
                daily_used  INTEGER DEFAULT 0,
                daily_reset BIGINT  DEFAULT 0,
                is_banned   INTEGER DEFAULT 0,
                refer_count INTEGER DEFAULT 0,
                referred_by BIGINT  DEFAULT NULL,
                username    TEXT    DEFAULT NULL,
                first_name  TEXT    DEFAULT NULL,
                country     TEXT    DEFAULT NULL,
                join_date   BIGINT  DEFAULT NULL
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS codes (
                code        TEXT    PRIMARY KEY,
                value       INTEGER NOT NULL,
                used        INTEGER DEFAULT 0,
                used_by     BIGINT  DEFAULT NULL,
                used_at     BIGINT  DEFAULT NULL,
                expiry      BIGINT  DEFAULT NULL,
                created_at  BIGINT  DEFAULT NULL
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS locked_data (
                query_text TEXT PRIMARY KEY,
                locked_at  BIGINT DEFAULT NULL
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS search_logs (
                id         SERIAL    PRIMARY KEY,
                uid        BIGINT,
                query      TEXT,
                mode       TEXT,
                country    TEXT      DEFAULT NULL,
                time_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # DB-11 FIX: auto-cleanup function for search_logs (keep last 90 days)
        cur.execute("""
            CREATE OR REPLACE FUNCTION cleanup_old_logs()
            RETURNS void LANGUAGE sql AS $$
                DELETE FROM search_logs WHERE time_stamp < NOW() - INTERVAL '90 days';
            $$
        """)

        migrations = [
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS country TEXT DEFAULT NULL",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS join_date BIGINT DEFAULT NULL",
            "ALTER TABLE codes ADD COLUMN IF NOT EXISTS used_by BIGINT DEFAULT NULL",
            "ALTER TABLE codes ADD COLUMN IF NOT EXISTS used_at BIGINT DEFAULT NULL",
            "ALTER TABLE codes ADD COLUMN IF NOT EXISTS created_at BIGINT DEFAULT NULL",
            "ALTER TABLE locked_data ADD COLUMN IF NOT EXISTS locked_at BIGINT DEFAULT NULL",
            "ALTER TABLE search_logs ADD COLUMN IF NOT EXISTS country TEXT DEFAULT NULL",
            "ALTER TABLE codes ADD COLUMN IF NOT EXISTS used INTEGER DEFAULT 0",
        ]
        for sql in migrations:
            try:
                cur.execute(sql)
            except Exception:
                pass

        index_sqls = [
            "CREATE INDEX IF NOT EXISTS idx_search_logs_uid ON search_logs(uid)",
            "CREATE INDEX IF NOT EXISTS idx_search_logs_time ON search_logs(time_stamp)",
            "CREATE INDEX IF NOT EXISTS idx_search_logs_mode ON search_logs(mode)",
            "CREATE INDEX IF NOT EXISTS idx_search_logs_country ON search_logs(country)",
            "CREATE INDEX IF NOT EXISTS idx_users_banned ON users(is_banned)",
        ]
        for sql in index_sqls:
            try:
                cur.execute(sql)
            except Exception:
                pass

        conn.commit()
        logger.info("✅ All tables & indexes ready")
        # Trigger log cleanup on startup (non-blocking, best-effort)
        try:
            cur.execute(
                "DELETE FROM search_logs WHERE time_stamp < NOW() - INTERVAL '90 days'"
            )
            conn.commit()
            logger.info("✅ Old log cleanup on startup done")
        except Exception:
            try: conn.rollback()
            except Exception: pass

    except Exception as exc:
        logger.error("setup_db error: %s", exc)
        conn.rollback()
    finally:
        if cur:
            cur.close()
        release(conn)


# ══════════════════════════════════════════
#  USER FUNCTIONS
# ══════════════════════════════════════════

def get_user(uid: int):
    """Fetch user row. Auto-creates if new. Auto-resets daily counter."""
    conn = get_conn()
    if not conn:
        return None
    cur = None
    try:
        cur = conn.cursor()
        now = int(time.time())

        cur.execute(
            "SELECT id, credits, daily_used, daily_reset, is_banned, refer_count "
            "FROM users WHERE id=%s",
            (uid,)
        )
        user = cur.fetchone()

        if not user:
            reset_ts = now + 86400
            cur.execute(
                "INSERT INTO users (id, credits, daily_used, daily_reset, join_date) "
                "VALUES (%s, 0, 0, %s, %s) ON CONFLICT (id) DO NOTHING",
                (uid, reset_ts, now)
            )
            conn.commit()
            cur.execute(
                "SELECT id, credits, daily_used, daily_reset, is_banned, refer_count "
                "FROM users WHERE id=%s",
                (uid,)
            )
            user = cur.fetchone()
            return user

        # Auto daily reset
        if user[3] and now > user[3]:
            cur.execute(
                "UPDATE users SET daily_used=0, daily_reset=%s WHERE id=%s",
                (now + 86400, uid)
            )
            conn.commit()
            cur.execute(
                "SELECT id, credits, daily_used, daily_reset, is_banned, refer_count "
                "FROM users WHERE id=%s",
                (uid,)
            )
            user = cur.fetchone()

        return user

    except Exception as exc:
        logger.error("get_user(%s): %s", uid, exc)
        try:
            conn.rollback()
        except Exception:
            pass
        if cur:
            try: cur.close()
            except Exception: pass
        release(conn, error=True)
        return None
    finally:
        if cur:
            try: cur.close()
            except Exception: pass
        release(conn)


def update_user(uid: int, field: str, value):
    """Whitelist-protected field update."""
    allowed = {"credits", "daily_used", "is_banned", "daily_reset"}
    if field not in allowed:
        logger.error("update_user: blocked field '%s'", field)
        return False
    conn = get_conn()
    if not conn:
        return False
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(f"UPDATE users SET {field}=%s WHERE id=%s", (value, uid))
        conn.commit()
        return True
    except Exception as exc:
        logger.error("update_user(%s, %s): %s", uid, field, exc)
        conn.rollback()
        return False
    finally:
        if cur:
            cur.close()
        release(conn)


def deduct_credit_atomic(uid: int, daily_limit: int) -> bool:
    """
    Atomic credit/daily deduction. Prevents race condition (TOCTOU fix).
    Returns True if deduction succeeded, False if no credits/limit left.
    """
    conn = get_conn()
    if not conn:
        return False
    cur = None
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE users SET
                daily_used = CASE
                    WHEN daily_used < %s THEN daily_used + 1
                    ELSE daily_used
                END,
                credits = CASE
                    WHEN daily_used >= %s AND credits > 0 THEN credits - 1
                    ELSE credits
                END
            WHERE id = %s
              AND (daily_used < %s OR credits > 0)
            RETURNING daily_used, credits
        """, (daily_limit, daily_limit, uid, daily_limit))
        result = cur.fetchone()
        conn.commit()
        return result is not None
    except Exception as exc:
        logger.error("deduct_credit_atomic(%s): %s", uid, exc)
        conn.rollback()
        return False
    finally:
        if cur:
            cur.close()
        release(conn)


def add_credits_to_user(uid: int, amount: int):
    """Add credits. Returns new balance or False."""
    conn = get_conn()
    if not conn:
        return False
    cur = None
    try:
        cur = conn.cursor()
        # DB-04 FIX: single time.time() call
        now = int(time.time())
        cur.execute(
            "INSERT INTO users (id, credits, daily_used, daily_reset, join_date) "
            "VALUES (%s, 0, 0, %s, %s) ON CONFLICT (id) DO NOTHING",
            (uid, now + 86400, now)
        )
        cur.execute(
            "UPDATE users SET credits = credits + %s WHERE id=%s RETURNING credits",
            (amount, uid)
        )
        result = cur.fetchone()
        conn.commit()
        return result[0] if result else False
    except Exception as exc:
        logger.error("add_credits_to_user(%s, %s): %s", uid, amount, exc)
        conn.rollback()
        return False
    finally:
        if cur:
            cur.close()
        release(conn)


def update_user_info(uid: int, username, first_name):
    # DB-05 FIX: cap string lengths before storing
    if username:
        username = str(username)[:64]
    if first_name:
        first_name = str(first_name)[:128]
    conn = get_conn()
    if not conn:
        return
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE users SET username=%s, first_name=%s WHERE id=%s",
            (username, first_name, uid)
        )
        conn.commit()
    except Exception as exc:
        logger.error("update_user_info(%s): %s", uid, exc)
        conn.rollback()
    finally:
        if cur:
            cur.close()
        release(conn)


def add_referral(new_uid: int, ref_id: int) -> bool:
    if new_uid == ref_id:
        return False
    conn = get_conn()
    if not conn:
        return False
    cur = None
    try:
        cur = conn.cursor()
        # DB-06 FIX: use FOR UPDATE to prevent race condition on referrer row
        # Check new user doesn't exist
        cur.execute("SELECT id FROM users WHERE id=%s FOR UPDATE", (new_uid,))
        if cur.fetchone():
            conn.rollback()
            return False
        # Lock referrer row
        cur.execute("SELECT id FROM users WHERE id=%s FOR UPDATE", (ref_id,))
        if not cur.fetchone():
            conn.rollback()
            return False
        cur.execute(
            "UPDATE users SET credits=credits+2, refer_count=refer_count+1 WHERE id=%s",
            (ref_id,)
        )
        now = int(time.time())
        cur.execute(
            "INSERT INTO users (id, credits, daily_used, daily_reset, referred_by, join_date) "
            "VALUES (%s, 0, 0, %s, %s, %s)",
            (new_uid, now + 86400, ref_id, now)
        )
        conn.commit()
        return True
    except Exception as exc:
        logger.error("add_referral(%s, %s): %s", new_uid, ref_id, exc)
        conn.rollback()
        return False
    finally:
        if cur:
            cur.close()
        release(conn)


def is_banned(uid: int) -> bool:
    conn = get_conn()
    if not conn:
        return False
    cur = None
    try:
        cur = conn.cursor()
        cur.execute("SELECT is_banned FROM users WHERE id=%s", (uid,))
        row = cur.fetchone()
        return row is not None and row[0] == 1
    except Exception as exc:
        logger.error("is_banned(%s): %s", uid, exc)
        return False
    finally:
        if cur:
            cur.close()
        release(conn)


def ban_user(uid: int):
    return update_user(uid, "is_banned", 1)


def unban_user(uid: int):
    return update_user(uid, "is_banned", 0)


def get_all_users():
    conn = get_conn()
    if not conn:
        return []
    cur = None
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM users WHERE is_banned=0")
        return cur.fetchall()
    except Exception as exc:
        logger.error("get_all_users: %s", exc)
        return []
    finally:
        if cur:
            cur.close()
        release(conn)


def get_all_users_detail():
    conn = get_conn()
    if not conn:
        return []
    cur = None
    try:
        cur = conn.cursor()
        # DB-10 FIX: add LIMIT 5000 — admin won't need more, prevents runaway
        cur.execute(
            "SELECT id, username, first_name, credits, is_banned, refer_count "
            "FROM users ORDER BY id ASC LIMIT 5000"
        )
        return cur.fetchall()
    except Exception as exc:
        logger.error("get_all_users_detail: %s", exc)
        return []
    finally:
        if cur:
            cur.close()
        release(conn)


def give_all_credits(amount: int) -> bool:
    conn = get_conn()
    if not conn:
        return False
    cur = None
    try:
        cur = conn.cursor()
        cur.execute("UPDATE users SET credits=credits+%s", (amount,))
        conn.commit()
        return True
    except Exception as exc:
        logger.error("give_all_credits: %s", exc)
        conn.rollback()
        return False
    finally:
        if cur:
            cur.close()
        release(conn)


# ══════════════════════════════════════════
#  PROMO CODES
# ══════════════════════════════════════════

def create_code(code: str, value: int, expiry=None) -> bool:
    conn = get_conn()
    if not conn:
        return False
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO codes (code, value, used, used_by, used_at, expiry, created_at) "
            "VALUES (%s, %s, 0, NULL, NULL, %s, %s) ON CONFLICT (code) DO NOTHING",
            (code, value, expiry, int(time.time()))
        )
        conn.commit()
        return cur.rowcount > 0
    except Exception as exc:
        logger.error("create_code: %s", exc)
        conn.rollback()
        return False
    finally:
        if cur:
            cur.close()
        release(conn)


def redeem_code(uid: int, code_text: str):
    """
    Returns: int credits | -1 already used | -2 expired | None invalid
    """
    conn = get_conn()
    if not conn:
        return None
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT value, used, expiry FROM codes WHERE code=%s FOR UPDATE",
            (code_text,)
        )
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return None
        if row[1] == 1:
            conn.rollback()
            return -1
        if row[2] is not None and int(time.time()) > row[2]:
            conn.rollback()
            return -2
        now = int(time.time())
        cur.execute(
            "UPDATE codes SET used=1, used_by=%s, used_at=%s WHERE code=%s",
            (uid, now, code_text)
        )
        cur.execute(
            "UPDATE users SET credits=credits+%s WHERE id=%s",
            (row[0], uid)
        )
        conn.commit()
        return row[0]
    except Exception as exc:
        logger.error("redeem_code(%s): %s", uid, exc)
        conn.rollback()
        return None
    finally:
        if cur:
            cur.close()
        release(conn)


def get_code_info(code_text: str):
    conn = get_conn()
    if not conn:
        return None
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT c.code, c.value, c.used, c.used_by, c.used_at, c.expiry, c.created_at, "
            "       u.username, u.first_name "
            "FROM codes c "
            "LEFT JOIN users u ON u.id = c.used_by "
            "WHERE c.code=%s",
            (code_text,)
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "code":       row[0],
            "value":      row[1],
            "used":       row[2],
            "used_by":    row[3],
            "used_at":    row[4],
            "expiry":     row[5],
            "created_at": row[6],
            "username":   row[7],
            "first_name": row[8],
        }
    except Exception as exc:
        logger.error("get_code_info: %s", exc)
        return None
    finally:
        if cur:
            cur.close()
        release(conn)


# ══════════════════════════════════════════
#  PRIVACY LOCK
# ══════════════════════════════════════════

def is_query_locked(query_text: str) -> bool:
    conn = get_conn()
    if not conn:
        return False
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM locked_data WHERE query_text=%s",
            (str(query_text).strip()[:500],)
        )
        return cur.fetchone() is not None
    except Exception as exc:
        logger.error("is_query_locked: %s", exc)
        return False
    finally:
        if cur:
            cur.close()
        release(conn)


def add_lock(query_text: str) -> bool:
    conn = get_conn()
    if not conn:
        return False
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO locked_data (query_text, locked_at) VALUES (%s, %s) "
            "ON CONFLICT DO NOTHING",
            (str(query_text).strip()[:500], int(time.time()))
        )
        conn.commit()
        return True
    except Exception as exc:
        logger.error("add_lock: %s", exc)
        conn.rollback()
        return False
    finally:
        if cur:
            cur.close()
        release(conn)


def remove_lock(query_text: str) -> bool:
    conn = get_conn()
    if not conn:
        return False
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM locked_data WHERE query_text=%s",
            (str(query_text).strip()[:500],)
        )
        conn.commit()
        return cur.rowcount > 0
    except Exception as exc:
        logger.error("remove_lock: %s", exc)
        conn.rollback()  # DB-12 FIX: was missing
        return False
    finally:
        if cur:
            cur.close()
        release(conn)


def get_locked_list():
    conn = get_conn()
    if not conn:
        return []
    cur = None
    try:
        cur = conn.cursor()
        # DB-07 FIX: LIMIT 1000
        cur.execute("SELECT query_text FROM locked_data ORDER BY locked_at DESC LIMIT 1000")
        return [row[0] for row in cur.fetchall()]
    except Exception as exc:
        logger.error("get_locked_list: %s", exc)
        return []
    finally:
        if cur:
            cur.close()
        release(conn)


# ══════════════════════════════════════════
#  SEARCH LOGS
# ══════════════════════════════════════════

def log_search(uid: int, query: str, mode: str, country: str = None):
    conn = get_conn()
    if not conn:
        return
    cur = None
    try:
        cur = conn.cursor()
        # DB-08 FIX: cap query length
        safe_query = str(query)[:500] if query else ""
        safe_mode  = str(mode)[:32] if mode else ""
        safe_country = str(country)[:64] if country else None
        cur.execute(
            "INSERT INTO search_logs (uid, query, mode, country) VALUES (%s, %s, %s, %s)",
            (uid, safe_query, safe_mode, safe_country)
        )
        conn.commit()
    except Exception as exc:
        logger.error("log_search: %s", exc)
        conn.rollback()
    finally:
        if cur:
            cur.close()
        release(conn)


def cleanup_old_logs(days: int = 90) -> int:
    """DB-11 FIX: Delete search logs older than N days. Call from a scheduled job."""
    conn = get_conn()
    if not conn:
        return 0
    cur = None
    try:
        cur = conn.cursor()
        # Use direct string format — psycopg2 can't parameterise INTERVAL units
        cur.execute(
            f"DELETE FROM search_logs WHERE time_stamp < NOW() - INTERVAL '{int(days)} days'"
        )
        deleted = cur.rowcount
        conn.commit()
        logger.info("cleanup_old_logs: deleted %d rows older than %d days", deleted, days)
        return deleted
    except Exception as exc:
        logger.error("cleanup_old_logs: %s", exc)
        conn.rollback()
        return 0
    finally:
        if cur:
            cur.close()
        release(conn)


def get_today_search_count() -> int:
    conn = get_conn()
    if not conn:
        return 0
    cur = None
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM search_logs WHERE time_stamp >= CURRENT_DATE")
        row = cur.fetchone()
        return row[0] if row else 0
    except Exception as exc:
        logger.error("get_today_search_count: %s", exc)
        return 0
    finally:
        if cur:
            cur.close()
        release(conn)


def get_search_stats_by_mode() -> dict:
    conn = get_conn()
    if not conn:
        return {}
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT mode, COUNT(*) FROM search_logs "
            "WHERE time_stamp >= CURRENT_DATE GROUP BY mode"
        )
        return {row[0]: row[1] for row in cur.fetchall()}
    except Exception as exc:
        logger.error("get_search_stats_by_mode: %s", exc)
        return {}
    finally:
        if cur:
            cur.close()
        release(conn)


def get_search_stats_by_country() -> list:
    conn = get_conn()
    if not conn:
        return []
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT country, COUNT(*) as cnt FROM search_logs "
            "WHERE country IS NOT NULL "
            "GROUP BY country ORDER BY cnt DESC LIMIT 10"
        )
        return cur.fetchall()
    except Exception as exc:
        logger.error("get_search_stats_by_country: %s", exc)
        return []
    finally:
        if cur:
            cur.close()
        release(conn)


def get_user_history(uid: int, limit: int = 50):
    # Cap limit at 100
    limit = min(int(limit), 100)
    conn = get_conn()
    if not conn:
        return []
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT query, mode, time_stamp FROM search_logs "
            "WHERE uid=%s ORDER BY time_stamp DESC LIMIT %s",
            (uid, limit)
        )
        return cur.fetchall()
    except Exception as exc:
        logger.error("get_user_history(%s): %s", uid, exc)
        return []
    finally:
        if cur:
            cur.close()
        release(conn)


def get_total_search_count() -> int:
    conn = get_conn()
    if not conn:
        return 0
    cur = None
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM search_logs")
        row = cur.fetchone()
        return row[0] if row else 0
    except Exception as exc:
        logger.error("get_total_search_count: %s", exc)
        return 0
    finally:
        if cur:
            cur.close()
        release(conn)
