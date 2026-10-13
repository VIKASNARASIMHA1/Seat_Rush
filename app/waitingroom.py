"""Virtual waiting room. FIFO queue in a Redis ZSET; the worker admits ADMIT_RATE users/second.
Admitted users get a TTL'd key; holds on waiting-room events require it."""
import secrets, time
from . import config

def _q(eid): return f"wr:q:{eid}"
def _ok(eid, ticket): return f"wr:ok:{eid}:{ticket}"

async def join(r, eid, user_id):
    key = f"wr:joined:{eid}:{user_id}"
    existing = await r.get(key)
    if existing: return existing
    ticket = f"{secrets.token_hex(8)}:{user_id}"
    if await r.set(key, ticket, nx=True, ex=config.ADMIT_TTL_SECONDS * 2):
        await r.zadd(_q(eid), {ticket: time.time()})
        return ticket
    return await r.get(key)

async def status(r, eid, ticket):
    if await r.exists(_ok(eid, ticket)): return {"admitted": True, "position": 0}
    rank = await r.zrank(_q(eid), ticket)
    if rank is None: return {"admitted": False, "position": None}
    return {"admitted": False, "position": rank + 1, "queue_length": await r.zcard(_q(eid))}

async def is_admitted(r, eid, ticket, user_id):
    return bool(ticket) and ticket.endswith(":" + user_id) and bool(await r.exists(_ok(eid, ticket)))

async def admit_batch(r, eid):
    popped = await r.zpopmin(_q(eid), config.ADMIT_RATE)   # atomic: safe with many workers
    if popped:
        pipe = r.pipeline()
        for ticket, _ in popped:
            pipe.set(_ok(eid, ticket), 1, ex=config.ADMIT_TTL_SECONDS)
        await pipe.execute()
    return len(popped)
