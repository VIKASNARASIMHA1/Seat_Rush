"""Read-heavy catalog with Redis caching and single-flight (stampede protection)."""
import asyncio, json

async def _single_flight(r, key, ttl, compute, lock_ttl=5):
    for _ in range(40):
        cached = await r.get(key)
        if cached is not None: return json.loads(cached)
        if await r.set(f"lock:{key}", 1, nx=True, ex=lock_ttl):    # one caller recomputes
            try:
                val = await compute()
                await r.set(key, json.dumps(val), ex=ttl)
                return val
            finally:
                await r.delete(f"lock:{key}")
        await asyncio.sleep(0.025)                                  # others wait for the winner
    return await compute()

def _ev(e): return {**dict(e), "starts_at": e["starts_at"].isoformat()}

async def get_event(pool, r, eid):
    async def compute():
        async with pool.acquire() as c:
            e = await c.fetchrow("SELECT id, name, venue, starts_at, waiting_room FROM events WHERE id=$1", eid)
        return None if not e else _ev(e)
    return await _single_flight(r, f"cache:event:{eid}", 30, compute)

async def list_events(pool, r):
    async def compute():
        async with pool.acquire() as c:
            rows = await c.fetch("SELECT id, name, venue, starts_at, waiting_room FROM events ORDER BY id")
        return [_ev(e) for e in rows]
    return await _single_flight(r, "cache:events", 30, compute)

async def availability(pool, r, eid):
    async def compute():
        async with pool.acquire() as c:
            rows = await c.fetch("SELECT status, count(*) n FROM seats WHERE event_id=$1 GROUP BY status", eid)
        d = {x["status"].lower(): x["n"] for x in rows}
        return {"available": d.get("available", 0), "held": d.get("held", 0), "sold": d.get("sold", 0)}
    return await _single_flight(r, f"cache:avail:{eid}", 1, compute)   # 1s TTL: bounded staleness
