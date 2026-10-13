"""Background worker. Every loop is safe to run in multiple replicas:
 - admission: ZPOPMIN is atomic            - reaper: idempotent UPDATE
 - outbox relay: FOR UPDATE SKIP LOCKED    - consumer: Redis Streams consumer group + idempotent sink"""
import asyncio, json, os, random, socket
from . import config, inventory, waitingroom
from .db import make_pool, make_redis

STREAM, DLQ, GROUP, MAX_ATTEMPTS = "events", "events:dlq", "notifier", 3
CONSUMER = f"{socket.gethostname()}-{os.getpid()}"

async def admission_loop(pool, r):
    while True:
        try:
            async with pool.acquire() as c:
                ids = [x["id"] for x in await c.fetch("SELECT id FROM events WHERE waiting_room")]
            for eid in ids: await waitingroom.admit_batch(r, eid)
        except Exception as e: print("admission error", e, flush=True)
        await asyncio.sleep(1)

async def reaper_loop(pool):
    while True:
        try:
            n = await inventory.reap_expired(pool)
            if n: print(f"reaper released {n} expired seats", flush=True)
        except Exception as e: print("reaper error", e, flush=True)
        await asyncio.sleep(5)

async def relay_loop(pool, r):
    """Transactional outbox -> Redis Stream. At-least-once: a crash between XADD and COMMIT re-publishes."""
    while True:
        try:
            async with pool.acquire() as c, c.transaction():
                rows = await c.fetch("SELECT id, topic, payload FROM outbox WHERE published_at IS NULL "
                                     "ORDER BY id LIMIT 200 FOR UPDATE SKIP LOCKED")
                for row in rows:
                    await r.xadd(STREAM, {"outbox_id": row["id"], "topic": row["topic"], "payload": row["payload"]},
                                 maxlen=config.STREAM_MAXLEN, approximate=True)
                if rows:
                    await c.execute("UPDATE outbox SET published_at=now() WHERE id=ANY($1::bigint[])", [x["id"] for x in rows])
            if not rows: await asyncio.sleep(0.2)
        except Exception as e:
            print("relay error", e, flush=True); await asyncio.sleep(1)

async def send_notification(pool, fields):
    """Idempotent sink: UNIQUE(event_key) makes redelivery harmless. Simulates a flaky email provider."""
    if random.random() < 0.15: raise RuntimeError("email provider 503")
    payload = json.loads(fields["payload"])
    body = f"order {payload.get('order_id')} {fields['topic']}"
    async with pool.acquire() as c:
        await c.execute("INSERT INTO notifications (event_key, user_id, kind, body) VALUES ($1,$2,$3,$4) "
                        "ON CONFLICT (event_key) DO NOTHING", fields["outbox_id"], payload.get("user_id"), fields["topic"], body)

async def handle(pool, r, msg_id, fields):
    try:
        await send_notification(pool, fields)
        await r.xack(STREAM, GROUP, msg_id)
        await r.hdel("notif:attempts", msg_id)
    except Exception:
        attempts = await r.hincrby("notif:attempts", msg_id, 1)
        if attempts >= MAX_ATTEMPTS:                      # poison message -> dead-letter queue
            await r.xadd(DLQ, {**fields, "failed_msg_id": msg_id}, maxlen=1000, approximate=True)
            await r.xack(STREAM, GROUP, msg_id)
            print(f"DLQ {msg_id}", flush=True)
        # else: leave pending; XAUTOCLAIM below retries after min-idle (retry w/ delay)

async def ensure_group(r):
    try: await r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    except Exception: pass                                # BUSYGROUP: already exists

async def consumer_loop(pool, r):
    await ensure_group(r)
    while True:
        try:
            res = await r.xautoclaim(STREAM, GROUP, CONSUMER, min_idle_time=3000, start_id="0-0", count=50)
            for msg_id, fields in res[1]:
                if fields: await handle(pool, r, msg_id, fields)
            resp = await r.xreadgroup(GROUP, CONSUMER, {STREAM: ">"}, count=50, block=1000)
            for _, msgs in resp or []:
                for msg_id, fields in msgs: await handle(pool, r, msg_id, fields)
        except Exception as e:
            print("consumer error", e, flush=True)
            if "NOGROUP" in str(e): await ensure_group(r)
            await asyncio.sleep(1)

def start_background(pool, r):
    """Used by the API process when RUN_WORKER_IN_API=1 (single free web service on Render)."""
    return [asyncio.create_task(c) for c in
            (admission_loop(pool, r), reaper_loop(pool), relay_loop(pool, r), consumer_loop(pool, r))]

async def main():
    pool, r = await make_pool(10), make_redis()
    print("worker started", flush=True)
    await asyncio.gather(admission_loop(pool, r), reaper_loop(pool), relay_loop(pool, r), consumer_loop(pool, r))

if __name__ == "__main__":
    asyncio.run(main())
