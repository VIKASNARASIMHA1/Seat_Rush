import uuid
from datetime import datetime, timedelta, timezone
from . import config

class SeatsUnavailable(Exception): pass

# FOR UPDATE SKIP LOCKED: contenders never block each other and never deadlock; a contested seat
# is simply "not available" for the loser. Expired holds are taken over atomically.
HOLD_SQL = """
WITH c AS (
  SELECT seat_id FROM seats
  WHERE event_id = $1 AND seat_id = ANY($2::text[])
    AND (status = 'AVAILABLE' OR (status = 'HELD' AND hold_expires_at < now()))
  ORDER BY seat_id FOR UPDATE SKIP LOCKED)
UPDATE seats s SET status = 'HELD', hold_id = $3, hold_expires_at = now() + make_interval(secs => $4)
FROM c WHERE s.event_id = $1 AND s.seat_id = c.seat_id
RETURNING s.seat_id, s.price_cents
"""

async def create_hold(pool, event_id, user_id, seat_ids):
    seat_ids = sorted(set(seat_ids))
    hold_id = uuid.uuid4()
    ttl = config.HOLD_TTL_SECONDS
    async with pool.acquire() as c:
        async with c.transaction():
            rows = await c.fetch(HOLD_SQL, event_id, seat_ids, hold_id, float(ttl))
            if len(rows) != len(seat_ids):
                raise SeatsUnavailable()          # raising rolls the whole transaction back
            expires = datetime.now(timezone.utc) + timedelta(seconds=ttl)
            await c.execute("INSERT INTO holds (id, event_id, user_id, seat_ids, expires_at) VALUES ($1,$2,$3,$4,$5)",
                            hold_id, event_id, user_id, seat_ids, expires)
    return {"hold_id": str(hold_id), "event_id": event_id, "seat_ids": seat_ids,
            "amount_cents": sum(r["price_cents"] for r in rows), "expires_at": expires.isoformat()}

async def release_hold(pool, hold_id, user_id):
    async with pool.acquire() as c, c.transaction():
        h = await c.fetchrow("SELECT * FROM holds WHERE id=$1 FOR UPDATE", hold_id)
        if not h or h["user_id"] != user_id or h["status"] != "ACTIVE":
            return False
        await c.execute("UPDATE seats SET status='AVAILABLE', hold_id=NULL, hold_expires_at=NULL "
                        "WHERE event_id=$1 AND hold_id=$2 AND status='HELD'", h["event_id"], hold_id)
        await c.execute("UPDATE holds SET status='RELEASED' WHERE id=$1", hold_id)
        return True

async def reap_expired(pool):
    async with pool.acquire() as c, c.transaction():
        n = await c.fetchval("WITH u AS (UPDATE seats SET status='AVAILABLE', hold_id=NULL, hold_expires_at=NULL "
                             "WHERE status='HELD' AND hold_expires_at < now() RETURNING 1) SELECT count(*) FROM u")
        await c.execute("UPDATE holds SET status='EXPIRED' WHERE status='ACTIVE' AND expires_at < now()")
        return n
