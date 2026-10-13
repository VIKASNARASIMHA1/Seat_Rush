import asyncpg, redis.asyncio as aioredis
from . import config

async def make_pool(max_size=None):
    return await asyncpg.create_pool(config.DATABASE_URL, min_size=1, max_size=max_size or config.DB_POOL_MAX,
                                     statement_cache_size=config.DB_STATEMENT_CACHE)

def make_redis():
    # BlockingConnectionPool queues callers when all connections are busy (instead of raising), so we can
    # stay under managed-Redis connection limits (Render free Key Value allows 50).
    pool = aioredis.BlockingConnectionPool.from_url(config.REDIS_URL, decode_responses=True,
                                                    max_connections=config.REDIS_MAX_CONN, timeout=10)
    return aioredis.Redis(connection_pool=pool)
