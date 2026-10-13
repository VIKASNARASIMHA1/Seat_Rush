"""Flash-sale simulation. Each virtual user: join waiting room -> wait for admission ->
pick seats -> hold -> checkout. Finishes by verifying invariants (no oversell, ledger reconciles).

  python loadtest/flash_sale.py --base-url http://localhost:8080 --event 2 --users 3000
"""
import argparse, asyncio, json, random, statistics, sys, time, uuid
import httpx

class Stats:
    def __init__(self):
        self.lat = {}; self.counts = {}
    def rec(self, name, dt, code):
        self.lat.setdefault(name, []).append(dt)
        k = f"{name}:{code}"; self.counts[k] = self.counts.get(k, 0) + 1

def pct(xs, p):
    xs = sorted(xs); return xs[min(len(xs) - 1, int(len(xs) * p))] * 1000 if xs else 0

async def call(client, st, name, method, url, **kw):
    for _ in range(8):                                   # honour 429/503 Retry-After like a polite client
        t = time.perf_counter()
        try:
            r = await client.request(method, url, **kw)
        except httpx.HTTPError:
            st.rec(name, time.perf_counter() - t, "neterr"); await asyncio.sleep(0.5); continue
        st.rec(name, time.perf_counter() - t, r.status_code)
        if r.status_code in (429, 503):
            await asyncio.sleep(float(r.headers.get("retry-after", 1)) * random.uniform(0.5, 1.5)); continue
        return r
    return None

async def user(i, a, client, st, sem, res):
    uid = f"user-{i}-{uuid.uuid4().hex[:6]}"
    h = {"x-user-id": uid}
    await asyncio.sleep(random.uniform(0, a.ramp))
    ticket = None
    if a.waiting_room:
        r = None
        while r is None or r.status_code != 200:
            r = await call(client, st, "queue_join", "POST", f"/events/{a.event}/queue/join", headers=h)
        ticket = r.json()["ticket"]
        t0 = time.time()
        while True:
            r = await call(client, st, "queue_status", "GET", f"/events/{a.event}/queue/status", params={"ticket": ticket}, headers=h)
            if r is not None and r.status_code == 200 and r.json()["admitted"]: break
            await asyncio.sleep(random.uniform(1.0, 2.0))
        res["wait"].append(time.time() - t0)
    hh = {**h, **({"x-queue-ticket": ticket} if ticket else {})}
    for attempt in range(a.attempts):
        async with sem:
            r = await call(client, st, "list_seats", "GET", f"/events/{a.event}/seats",
                           params={"after": f"S{random.randint(0, a.sections - 1):03d}-R{random.randint(0, 9):02d}-00", "limit": 30})
            seats = r.json()["seats"] if r and r.status_code == 200 else []
            if not seats:
                r = await call(client, st, "list_seats", "GET", f"/events/{a.event}/seats", params={"limit": 30})
                seats = r.json()["seats"] if r and r.status_code == 200 else []
        if not seats:
            res["soldout"] += 1; return
        pick = [s["seat_id"] for s in random.sample(seats, min(random.randint(1, 3), len(seats)))]
        async with sem:
            r = await call(client, st, "hold", "POST", f"/events/{a.event}/holds", headers=hh, json={"seat_ids": pick})
        if r is None or r.status_code != 201:
            res["hold_conflict"] += 1; continue
        hold = r.json()
        await asyncio.sleep(random.uniform(0.05, 0.3))  # "typing card number"
        async with sem:
            r = await call(client, st, "order", "POST", "/orders", headers={**h, "idempotency-key": uuid.uuid4().hex},
                           json={"hold_id": hold["hold_id"]})
        if r is not None and r.status_code == 201 and r.json()["status"] == "CONFIRMED":
            res["bought"] += 1; return
        res["order_failed"] += 1                          # declined etc.: seats released, user retries

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default="http://localhost:8080")
    ap.add_argument("--event", type=int, default=2)
    ap.add_argument("--users", type=int, default=3000)
    ap.add_argument("--concurrency", type=int, default=200)
    ap.add_argument("--attempts", type=int, default=8)
    ap.add_argument("--ramp", type=float, default=5.0)
    ap.add_argument("--out", default="loadtest_report.json")
    a = ap.parse_args()
    st, sem = Stats(), asyncio.Semaphore(a.concurrency)
    res = {"bought": 0, "hold_conflict": 0, "order_failed": 0, "soldout": 0, "wait": []}
    async with httpx.AsyncClient(base_url=a.base_url, timeout=30,
                                 limits=httpx.Limits(max_connections=a.concurrency * 2)) as client:
        ev = (await client.get(f"/events/{a.event}")).json()
        a.waiting_room = ev["waiting_room"]
        before = (await client.get(f"/admin/stats/{a.event}")).json()
        total = sum(before["seats"].values()); a.sections = max(1, total // 200)
        print(f"event '{ev['name']}' seats={total} users={a.users} waiting_room={a.waiting_room}")
        t0 = time.time()
        await asyncio.gather(*(user(i, a, client, st, sem, res) for i in range(a.users)))
        dur = time.time() - t0
        await asyncio.sleep(6)                            # let outbox relay + notifier drain
        s = (await client.get(f"/admin/stats/{a.event}")).json()
    print(f"\n=== flash sale finished in {dur:.1f}s ===")
    print(f"users bought: {res['bought']}  hold conflicts: {res['hold_conflict']}  order failures: {res['order_failed']}  sold-out exits: {res['soldout']}")
    if res["wait"]: print(f"waiting room wait  p50={statistics.median(res['wait']):.1f}s  max={max(res['wait']):.1f}s")
    print(f"{'endpoint':<14}{'n':>8}{'p50 ms':>9}{'p95 ms':>9}{'p99 ms':>9}")
    for k, v in sorted(st.lat.items()):
        print(f"{k:<14}{len(v):>8}{pct(v,.5):>9.1f}{pct(v,.95):>9.1f}{pct(v,.99):>9.1f}")
    print("status codes:", json.dumps(dict(sorted(st.counts.items()))))
    print("\nserver stats:", json.dumps({k: s[k] for k in ("seats", "orders_by_status", "failed_reasons", "outbox_backlog", "notifications")}))
    print("invariants:", json.dumps(s["invariants"]))
    ok = all(s["invariants"].values())
    print("RESULT:", "PASS - zero oversells, ledger reconciles" if ok else "FAIL")
    json.dump({"duration_s": dur, "client": {k: v for k, v in res.items() if k != "wait"}, "server": s,
               "latency_ms": {k: {"n": len(v), "p50": pct(v, .5), "p95": pct(v, .95), "p99": pct(v, .99)} for k, v in st.lat.items()}},
              open(a.out, "w"), indent=2, default=str)
    sys.exit(0 if ok else 1)

asyncio.run(main())
