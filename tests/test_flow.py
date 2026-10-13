"""Integration tests against a running stack:  pytest -q   (BASE_URL defaults to http://localhost:8080)"""
import asyncio, os, uuid
import httpx

BASE = os.getenv("BASE_URL", "http://localhost:8080")
EVENT = 1  # event 1 has no waiting room

def uid(): return "t-" + uuid.uuid4().hex[:8]

def free_seats(n=1):
    r = httpx.get(f"{BASE}/events/{EVENT}/seats", params={"limit": 100, "after": f"S00{os.getpid() % 5}-R0{os.getpid() % 9}-00"}, headers={"x-user-id": uid()})
    assert r.status_code == 200
    seats = [s["seat_id"] for s in r.json()["seats"]]
    assert len(seats) >= n, "not enough free seats; reseed with --reset"
    return seats

def test_hold_is_exclusive_under_race():
    """50 users race for the same seat: exactly one wins, 49 get 409."""
    seat = free_seats()[0]
    async def go():
        async with httpx.AsyncClient(base_url=BASE, timeout=30) as c:
            rs = await asyncio.gather(*(c.post(f"/events/{EVENT}/holds", json={"seat_ids": [seat]},
                                               headers={"x-user-id": uid()}) for _ in range(50)))
        return [r.status_code for r in rs]
    codes = asyncio.run(go())
    assert codes.count(201) == 1, codes
    assert codes.count(409) + codes.count(429) == 49, codes

def test_order_is_idempotent():
    seat = free_seats()[-1]; user = uid(); h = {"x-user-id": user}
    hold = httpx.post(f"{BASE}/events/{EVENT}/holds", json={"seat_ids": [seat]}, headers=h)
    assert hold.status_code == 201
    key = {**h, "idempotency-key": uuid.uuid4().hex}
    body = {"hold_id": hold.json()["hold_id"]}
    a = httpx.post(f"{BASE}/orders", json=body, headers=key, timeout=30)
    b = httpx.post(f"{BASE}/orders", json=body, headers=key, timeout=30)
    assert a.status_code == b.status_code == 201
    assert a.json()["order_id"] == b.json()["order_id"]

def test_waiting_room_blocks_unadmitted_users():
    u = uid()
    r = httpx.post(f"{BASE}/events/2/holds", json={"seat_ids": ["S000-R00-00"]}, headers={"x-user-id": u})
    assert r.status_code == 403

def test_all_invariants_hold():
    s = httpx.get(f"{BASE}/admin/stats/{EVENT}").json()
    assert all(s["invariants"].values()), s
