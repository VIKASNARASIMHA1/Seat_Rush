import time, uuid
from contextlib import asynccontextmanager
from fastapi import FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel, Field
from . import catalog, config, inventory, orders, payments, ratelimit, seed, waitingroom, worker
from .db import make_pool, make_redis

HOLDS = Counter("tm_holds_total", "Hold attempts", ["result"])
ORDERS = Counter("tm_orders_total", "Orders by final status", ["status"])
LIMITED = Counter("tm_rate_limited_total", "Requests rejected by rate limiter")
SHED = Counter("tm_load_shed_total", "Requests shed due to overload")
LATENCY = Histogram("tm_request_seconds", "Request latency", ["route", "method"])

pool = None
redis = None
inflight = 0

@asynccontextmanager
async def lifespan(app):
    global pool, redis
    if config.AUTO_SEED:
        await seed.main()                      # idempotent; takes an advisory lock on a direct connection
    pool, redis = await make_pool(), make_redis()
    tasks = worker.start_background(pool, redis) if config.RUN_WORKER_IN_API else []
    yield
    for t in tasks: t.cancel()
    await pool.close(); await redis.aclose()

app = FastAPI(title="SeatRush", lifespan=lifespan)

@app.middleware("http")
async def guard(request: Request, call_next):
    """Edge protections: per-caller token bucket (429) and global in-flight cap (503 load shedding)."""
    global inflight
    path = request.url.path
    if path in ("/health", "/metrics") or path.startswith("/admin"):
        return await call_next(request)
    caller = request.headers.get("x-user-id") or (request.client.host if request.client else "anon")
    if not await ratelimit.allow(redis, f"rl:{caller}"):
        LIMITED.inc()
        return JSONResponse({"detail": "rate limited"}, status_code=429, headers={"Retry-After": "1"})
    if inflight >= config.MAX_INFLIGHT:
        SHED.inc()
        return JSONResponse({"detail": "overloaded, retry shortly"}, status_code=503, headers={"Retry-After": "2"})
    inflight += 1
    t0 = time.perf_counter()
    try:
        resp = await call_next(request)
    finally:
        inflight -= 1
    route = request.scope.get("route")
    LATENCY.labels(route.path if route else "unmatched", request.method).observe(time.perf_counter() - t0)
    return resp

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/events")
async def events():
    return await catalog.list_events(pool, redis)

@app.get("/events/{event_id}")
async def event(event_id: int):
    e = await catalog.get_event(pool, redis, event_id)
    if not e: raise HTTPException(404, "event not found")
    return e

@app.get("/events/{event_id}/availability")
async def availability(event_id: int):
    return await catalog.availability(pool, redis, event_id)

@app.get("/events/{event_id}/seats")
async def seats(event_id: int, after: str = "", limit: int = 20):
    """Cursor-paginated list of available seats (keyset pagination on seat_id)."""
    limit = max(1, min(limit, 100))
    async with pool.acquire() as c:
        rows = await c.fetch("SELECT seat_id, price_cents FROM seats WHERE event_id=$1 AND status='AVAILABLE' "
                             "AND seat_id > $2 ORDER BY seat_id LIMIT $3", event_id, after, limit)
    return {"seats": [dict(r) for r in rows], "next_after": rows[-1]["seat_id"] if rows else None}

@app.post("/events/{event_id}/queue/join")
async def queue_join(event_id: int, x_user_id: str = Header(...)):
    ticket = await waitingroom.join(redis, event_id, x_user_id)
    return {"ticket": ticket, **await waitingroom.status(redis, event_id, ticket)}

@app.get("/events/{event_id}/queue/status")
async def queue_status(event_id: int, ticket: str):
    return await waitingroom.status(redis, event_id, ticket)

class HoldIn(BaseModel):
    seat_ids: list[str] = Field(min_length=1, max_length=config.MAX_SEATS_PER_HOLD)

@app.post("/events/{event_id}/holds", status_code=201)
async def create_hold(event_id: int, body: HoldIn, x_user_id: str = Header(...),
                      x_queue_ticket: str | None = Header(None)):
    ev = await catalog.get_event(pool, redis, event_id)
    if not ev: raise HTTPException(404, "event not found")
    if ev["waiting_room"] and not await waitingroom.is_admitted(redis, event_id, x_queue_ticket, x_user_id):
        HOLDS.labels("not_admitted").inc()
        raise HTTPException(403, "join the waiting room and wait to be admitted")
    try:
        h = await inventory.create_hold(pool, event_id, x_user_id, body.seat_ids)
    except inventory.SeatsUnavailable:
        HOLDS.labels("conflict").inc()
        raise HTTPException(409, "one or more seats are unavailable")
    HOLDS.labels("ok").inc()
    return h

@app.delete("/holds/{hold_id}")
async def delete_hold(hold_id: uuid.UUID, x_user_id: str = Header(...)):
    if not await inventory.release_hold(pool, hold_id, x_user_id): raise HTTPException(404, "no active hold")
    return {"released": True}

class OrderIn(BaseModel):
    hold_id: uuid.UUID

@app.post("/orders", status_code=201)
async def create_order(body: OrderIn, x_user_id: str = Header(...), idempotency_key: str = Header(...)):
    try:
        o = await orders.place_order(pool, redis, x_user_id, body.hold_id, idempotency_key)
    except orders.OrderError as e:
        raise HTTPException(e.status, e.msg)
    ORDERS.labels(o["status"]).inc()
    return o

@app.get("/orders/{order_id}")
async def get_order(order_id: uuid.UUID):
    o = await orders.get_order(pool, order_id)
    if not o: raise HTTPException(404, "order not found")
    return o

@app.get("/admin/stats/{event_id}")
async def stats(event_id: int):
    """Invariant checks used by the load test: no oversell, ledger reconciles with orders."""
    async with pool.acquire() as c:
        seat_counts = {r["status"].lower(): r["n"] for r in await c.fetch(
            "SELECT status, count(*) n FROM seats WHERE event_id=$1 GROUP BY status", event_id)}
        confirmed = await c.fetchrow("SELECT count(*) n, coalesce(sum(cardinality(seat_ids)),0) seats, "
                                     "coalesce(sum(amount_cents),0) cents FROM orders WHERE event_id=$1 AND status='CONFIRMED'", event_id)
        statuses = {r["status"]: r["n"] for r in await c.fetch(
            "SELECT status, count(*) n FROM orders WHERE event_id=$1 GROUP BY status", event_id)}
        reasons = {r["failure_reason"]: r["n"] for r in await c.fetch(
            "SELECT failure_reason, count(*) n FROM orders WHERE event_id=$1 AND status='FAILED' GROUP BY 1", event_id)}
        dup = await c.fetchval("SELECT count(*) FROM (SELECT unnest(seat_ids) s FROM orders WHERE event_id=$1 AND status='CONFIRMED' "
                               "GROUP BY 1 HAVING count(*) > 1) x", event_id)
        all_confirmed_cents = await c.fetchval("SELECT coalesce(sum(amount_cents),0) FROM orders WHERE status='CONFIRMED'")
        outbox_backlog = await c.fetchval("SELECT count(*) FROM outbox WHERE published_at IS NULL")
        notifs = await c.fetchval("SELECT count(*) FROM notifications")
    net = await payments.net_captured_cents(redis)
    sold = seat_counts.get("sold", 0)
    return {"seats": seat_counts, "orders_by_status": statuses, "failed_reasons": reasons,
            "confirmed_orders": confirmed["n"], "confirmed_seats": confirmed["seats"],
            "confirmed_cents": confirmed["cents"], "net_captured_cents": net,
            "outbox_backlog": outbox_backlog, "notifications": notifs,
            "invariants": {"no_oversell_sold_eq_confirmed_seats": sold == confirmed["seats"],
                           "no_seat_in_two_orders": dup == 0,
                           "payments_reconcile": net == all_confirmed_cents}}   # ledger is global, so compare to all events
