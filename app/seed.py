"""Create schema and seed demo events. Usage: python -m app.seed [--reset]"""
import asyncio, math, os, sys
from datetime import datetime, timedelta, timezone
import asyncpg
from . import config
from .db import make_redis

SEATS_PER_SECTION = 200  # 10 rows x 20 seats

def seat_rows(event_id, n):
    out = []
    for i in range(n):
        s, rem = divmod(i, SEATS_PER_SECTION)
        row, num = divmod(rem, 20)
        price = 15000 if s == 0 else 9000 if s < 3 else 5000
        out.append((event_id, f"S{s:03d}-R{row:02d}-{num:02d}", price))
    return out

async def main(reset=False):
    conn = await asyncpg.connect(config.DIRECT_DATABASE_URL)
    await conn.execute("SELECT pg_advisory_lock(424242)")
    try:
        with open(os.path.join(os.path.dirname(__file__), "schema.sql")) as f:
            await conn.execute(f.read())
        if reset:
            await conn.execute("TRUNCATE notifications, outbox, orders, holds, seats, events RESTART IDENTITY CASCADE")
            r = make_redis(); await r.flushdb(); await r.aclose()
            print("reset done")
        if await conn.fetchval("SELECT count(*) FROM events") == 0:
            start = datetime.now(timezone.utc) + timedelta(days=30)
            await conn.execute("INSERT INTO events (name, venue, starts_at, waiting_room) VALUES "
                               "('Open Sale Concert','City Hall',$1,false),"
                               "('Mega Tour Flash Sale','Stadium',$1,true)", start)
            n = config.SEED_SEATS
            for ev in (1, 2):
                await conn.copy_records_to_table("seats", records=seat_rows(ev, n),
                    columns=["event_id", "seat_id", "price_cents"])
            print(f"seeded 2 events x {n} seats ({math.ceil(n/SEATS_PER_SECTION)} sections)")
        else:
            print("already seeded")
    finally:
        await conn.execute("SELECT pg_advisory_unlock(424242)")
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main("--reset" in sys.argv))
