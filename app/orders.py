"""Checkout saga: create PENDING order -> charge (idempotent) -> confirm seats in one tx.
Compensation: any failure after charge refunds and releases seats. Events go through the outbox."""
import json, uuid
from datetime import datetime, timezone
from . import payments

class OrderError(Exception):
    def __init__(self, status, msg): self.status, self.msg = status, msg

class _HoldLost(Exception): pass

def _row(o):
    return {"order_id": str(o["id"]), "status": o["status"], "event_id": o["event_id"],
            "seat_ids": list(o["seat_ids"]), "amount_cents": o["amount_cents"],
            "payment_ref": o["payment_ref"], "failure_reason": o["failure_reason"]}

async def _outbox(c, topic, payload):
    await c.execute("INSERT INTO outbox (topic, payload) VALUES ($1, $2::jsonb)", topic, json.dumps(payload))

async def get_order(pool, order_id):
    async with pool.acquire() as c:
        o = await c.fetchrow("SELECT * FROM orders WHERE id=$1", order_id)
    return _row(o) if o else None

async def place_order(pool, redis, user_id, hold_id, idem_key):
    async with pool.acquire() as c:
        existing = await c.fetchrow("SELECT * FROM orders WHERE idempotency_key=$1", idem_key)
        if existing:                                    # idempotent replay
            if existing["user_id"] != user_id: raise OrderError(409, "idempotency key belongs to another user")
            return _row(existing)
        hold = await c.fetchrow("SELECT * FROM holds WHERE id=$1", hold_id)
        if not hold or hold["user_id"] != user_id: raise OrderError(404, "hold not found")
        if hold["status"] != "ACTIVE" or hold["expires_at"] < datetime.now(timezone.utc):
            raise OrderError(410, "hold expired")
        seat_ids = list(hold["seat_ids"])
        amount = await c.fetchval("SELECT sum(price_cents) FROM seats WHERE event_id=$1 AND seat_id=ANY($2::text[]) "
                                  "AND hold_id=$3", hold["event_id"], seat_ids, hold_id)
        n = await c.fetchval("SELECT count(*) FROM seats WHERE hold_id=$1", hold_id)
        if n != len(seat_ids): raise OrderError(410, "hold expired")
        order_id = uuid.uuid4()
        ins = await c.fetchval("INSERT INTO orders (id, idempotency_key, hold_id, user_id, event_id, seat_ids, amount_cents, status) "
                               "VALUES ($1,$2,$3,$4,$5,$6,$7,'PENDING') ON CONFLICT DO NOTHING RETURNING id",
                               order_id, idem_key, hold_id, user_id, hold["event_id"], seat_ids, amount)
        if ins is None:
            ex = await c.fetchrow("SELECT * FROM orders WHERE idempotency_key=$1", idem_key)
            if ex: return _row(ex)
            raise OrderError(409, "hold already has an order")

    ref, reason = None, None
    try:                                                # step 2: charge (outside any DB tx)
        for attempt in range(2):
            try:
                ref = await payments.charge(redis, idem_key, amount); break
            except payments.PaymentTimeout:
                if attempt == 1: raise                  # same key => provider dedupes, no double charge
    except payments.PaymentDeclined:
        reason = "payment_declined"
    except payments.PaymentTimeout:
        reason = "payment_timeout"

    async with pool.acquire() as c:
        if reason is None:                              # step 3: confirm seats + order atomically
            try:
                async with c.transaction():
                    sold = await c.fetchval(
                        "WITH u AS (UPDATE seats SET status='SOLD', order_id=$4, hold_id=NULL, hold_expires_at=NULL "
                        "WHERE event_id=$1 AND seat_id=ANY($2::text[]) AND hold_id=$3 AND status='HELD' RETURNING 1) "
                        "SELECT count(*) FROM u", hold["event_id"], seat_ids, hold_id, order_id)
                    if sold != len(seat_ids): raise _HoldLost()
                    await c.execute("UPDATE orders SET status='CONFIRMED', payment_ref=$2 WHERE id=$1", order_id, ref)
                    await c.execute("UPDATE holds SET status='CONSUMED' WHERE id=$1", hold_id)
                    await _outbox(c, "order.confirmed", {"order_id": str(order_id), "user_id": user_id,
                                  "event_id": hold["event_id"], "seat_ids": seat_ids, "amount_cents": amount})
            except _HoldLost:
                await payments.refund(redis, ref)       # compensation: hold expired and was taken over
                reason = "hold_lost_refunded"
        if reason is not None:                          # failure path: release seats, mark failed
            async with c.transaction():
                await c.execute("UPDATE seats SET status='AVAILABLE', hold_id=NULL, hold_expires_at=NULL "
                                "WHERE event_id=$1 AND hold_id=$2 AND status='HELD'", hold["event_id"], hold_id)
                await c.execute("UPDATE holds SET status='RELEASED' WHERE id=$1", hold_id)
                await c.execute("UPDATE orders SET status='FAILED', failure_reason=$2 WHERE id=$1", order_id, reason)
                await _outbox(c, "order.failed", {"order_id": str(order_id), "user_id": user_id, "reason": reason})
        o = await c.fetchrow("SELECT * FROM orders WHERE id=$1", order_id)
    return _row(o)
