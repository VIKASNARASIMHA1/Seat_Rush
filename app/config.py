import os
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

def _f(name, default): return float(os.getenv(name, default))
def _i(name, default): return int(os.getenv(name, default))

def _clean_dsn(dsn):
    """Neon URLs carry libpq-only params (channel_binding) that asyncpg would forward as server settings."""
    u = urlparse(dsn)
    q = [(k, v) for k, v in parse_qsl(u.query) if k != "channel_binding"]
    return urlunparse(u._replace(query=urlencode(q)))

DATABASE_URL = _clean_dsn(os.getenv("DATABASE_URL", "postgresql://tm:tm@localhost:5432/tm"))
# Neon: DATABASE_URL = pooled (-pooler) host for the app; DIRECT_DATABASE_URL = direct host for seeding (advisory locks)
DIRECT_DATABASE_URL = _clean_dsn(os.getenv("DIRECT_DATABASE_URL", DATABASE_URL))
DB_STATEMENT_CACHE = _i("DB_STATEMENT_CACHE", 100)   # set 0 behind PgBouncer (Neon pooler)
RUN_WORKER_IN_API = os.getenv("RUN_WORKER_IN_API", "0") == "1"
AUTO_SEED = os.getenv("AUTO_SEED", "0") == "1"
REDIS_MAX_CONN = _i("REDIS_MAX_CONN", 30)
STREAM_MAXLEN = _i("STREAM_MAXLEN", 20000)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
SEED_SEATS = _i("SEED_SEATS", 2000)
HOLD_TTL_SECONDS = _f("HOLD_TTL_SECONDS", 120)
MAX_SEATS_PER_HOLD = _i("MAX_SEATS_PER_HOLD", 4)
ADMIT_RATE = _i("ADMIT_RATE", 200)            # users admitted per second per event
ADMIT_TTL_SECONDS = _i("ADMIT_TTL_SECONDS", 600)
RATE_LIMIT_RPS = _f("RATE_LIMIT_RPS", 30)     # per user (or IP) token bucket
RATE_LIMIT_BURST = _f("RATE_LIMIT_BURST", 60)
MAX_INFLIGHT = _i("MAX_INFLIGHT", 400)        # load shedding threshold per api process
PAYMENT_DECLINE_RATE = _f("PAYMENT_DECLINE_RATE", 0.10)
PAYMENT_TIMEOUT_RATE = _f("PAYMENT_TIMEOUT_RATE", 0.05)
DB_POOL_MAX = _i("DB_POOL_MAX", 30)
