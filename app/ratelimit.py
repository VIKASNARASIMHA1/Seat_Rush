"""Distributed token bucket in Redis (atomic Lua). Fails open if Redis is unavailable."""
from . import config

LUA = """
local t = redis.call('TIME'); local now = t[1] + t[2] / 1000000
local rate = tonumber(ARGV[1]); local cap = tonumber(ARGV[2])
local d = redis.call('HMGET', KEYS[1], 'tok', 'ts')
local tok = tonumber(d[1]) or cap; local ts = tonumber(d[2]) or now
tok = math.min(cap, tok + (now - ts) * rate)
local ok = 0
if tok >= 1 then tok = tok - 1; ok = 1 end
redis.call('HSET', KEYS[1], 'tok', tostring(tok), 'ts', tostring(now))
redis.call('EXPIRE', KEYS[1], math.ceil(cap / rate) * 2 + 1)
return ok
"""

_sha = None

async def allow(r, key):
    global _sha
    try:
        if _sha is None:
            _sha = await r.script_load(LUA)
        ok = await r.evalsha(_sha, 1, key, config.RATE_LIMIT_RPS, config.RATE_LIMIT_BURST)
        return bool(ok)
    except Exception:
        _sha = None
        return True  # graceful degradation: a dead limiter must not take the site down
