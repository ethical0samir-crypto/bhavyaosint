"""
database.py — Thread-safe PostgreSQL layer for LaceraOSINT
Uses ThreadedConnectionPool (NOT SimpleConnectionPool).
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

DATABASE_URL = os.getenv("DATABASE_URL", "")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

try:
    db_pool = pg_pool.ThreadedConnectionPool(
        minconn=2, maxconn=25,
        dsn=DATABASE_URL,
        sslmode="require",
        connect_timeout=10,
    )
    logger.info("✅ DB pool (Threaded) created — min=2 max=25")
except Exception as exc:
    logger.critical("❌ DB pool failed: %s", exc)
    db_pool = None  # type: ignore


def get_conn():
    if not db_pool:
        logger.error("get_conn: pool is None")
        return None
    try:
        conn = db_pool.getconn()
        if conn is None:
            return None
        # Health check — detect stale/broken connections
        if conn.closed:
            logger.warning("get_conn: stale closed connection, discarding")
            try:
                db_pool.putconn(conn, close=True)
            except Exception:
                pass
            return db_pool.getconn()
        return conn
    except Exception as exc:
        logger.error("get_conn error: %s", exc)
        return None


def release(conn):
    if db_pool and conn:
        try:
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

        # Graceful column additions for existing DBs
        migrations = [
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS country TEXT DEFAULT NULL",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS join_date BIGINT DEFAULT NULL",
            "ALTER TABLE codes ADD COLUMN IF NOT EXISTS used_by BIGINT DEFAULT NULL",
            "ALTER TABLE codes ADD COLUMN IF NOT EXISTS used_at BIGINT DEFAULT NULL",
            "ALTER TABLE codes ADD COLUMN IF NOT EXISTS created_at BIGINT DEFAULT NULL",
            "ALTER TABLE locked_data ADD COLUMN IF NOT EXISTS locked_at BIGINT DEFAULT NULL",
            "ALTER TABLE search_logs ADD COLUMN IF NOT EXISTS country TEXT DEFAULT NULL",
            # Drop old 'used' column if it exists (replaced by used_by)
            # Not dropping to keep backwards compat — codes with used=1 still work
            "ALTER TABLE codes ADD COLUMN IF NOT EXISTS used INTEGER DEFAULT 0",
        ]
        for sql in migrations:
            try:
                cur.execute(sql)
            except Exception:
                pass

        # Indexes for performance
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
        if now > user[3]:
            cur.execute(
                "UPDATE users SET daily_used=0, daily_reset=%s WHERE id=%s",
                (now + 86400, uid)
            )
            conn.commit()
            # Fresh SELECT after reset — avoids fragile manual tuple reconstruction
            cur.execute(
                "SELECT id, credits, daily_used, daily_reset, is_banned, refer_count "
                "FROM users WHERE id=%s",
                (uid,)
            )
            user = cur.fetchone()

        return user

    except Exception as exc:
        logger.error("get_user(%s): %s", uid, exc)
        conn.rollback()
        return None
    finally:
        if cur:
            cur.close()
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
        # Single atomic query: deduct daily_used if under limit, else deduct credit
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
        cur.execute(
            "INSERT INTO users (id, credits, daily_used, daily_reset, join_date) "
            "VALUES (%s, 0, 0, %s, %s) ON CONFLICT (id) DO NOTHING",
            (uid, int(time.time()) + 86400, int(time.time()))
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
        # New user must not already exist
        cur.execute("SELECT id FROM users WHERE id=%s", (new_uid,))
        if cur.fetchone():
            return False
        # Referrer must exist
        cur.execute("SELECT id FROM users WHERE id=%s", (ref_id,))
        if not cur.fetchone():
            return False
        # Atomic: give referrer +2 credits
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
        cur.execute(
            "SELECT id, username, first_name, credits, is_banned, refer_count "
            "FROM users ORDER BY id ASC"
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
#  PROMO CODES — v2 (tracks who used it)
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
    Also records who redeemed it.
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
            return None
        if row[1] == 1:
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
    """
    Returns full info about a code for /usedcode command.
    Returns dict or None.
    """
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
            (str(query_text).strip(),)
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
            (str(query_text).strip(), int(time.time()))
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
            (str(query_text).strip(),)
        )
        conn.commit()
        return cur.rowcount > 0
    except Exception as exc:
        logger.error("remove_lock: %s", exc)
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
        cur.execute("SELECT query_text FROM locked_data ORDER BY query_text")
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
        cur.execute(
            "INSERT INTO search_logs (uid, query, mode, country) VALUES (%s, %s, %s, %s)",
            (uid, str(query), mode, country)
        )
        conn.commit()
    except Exception as exc:
        logger.error("log_search: %s", exc)
        conn.rollback()
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
        cur.execute(
            "SELECT COUNT(*) FROM search_logs WHERE time_stamp >= CURRENT_DATE"
        )
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
    """Returns count per mode for /stats command."""
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
    """Top countries by search count (all time)."""
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
