# SeatRush

A flash-sale ticketing backend that **never sells the same seat twice**, even when thousands of buyers hit it at once.
Python 3.12 / FastAPI / Postgres ([Neon](https://neon.com)) / Redis ([Render Key Value](https://render.com)).
Runs entirely in the cloud: **no Docker or database needs to be installed on your PC.**

> A systems-design portfolio project. It is an API only (no buyer UI); payments and emails are simulated.

## What problem does it solve?

When a big event goes on sale, naive systems oversell seats, fall over under load, charge people for tickets they don't get,
or double-charge on retries. SeatRush prevents each failure and **proves it with a load test**.

| Failure | How it is prevented |
|---|---|
| Two buyers get the same seat | One atomic DB reservation (`UPDATE ... FOR UPDATE SKIP LOCKED`): exactly one winner, losers get `409` instantly |
| Servers flooded | Waiting room (admits N users/sec), per-user rate limit (`429`), overload shedding (`503`) |
| Seats stuck on people who never pay | Holds expire after 2 minutes and are swept by a background loop |
| Charged but no ticket | Checkout saga: if a later step fails the seat is released and the charge refunded |
| Double charge on retry | `Idempotency-Key` header plus idempotent payment calls: the same request returns the same order |
| Lost notifications | Transactional outbox -> Redis Stream -> consumer with retries and a dead-letter queue |

## How a purchase flows

1. **Browse:** `GET /events`, `/events/{id}/availability`, `/events/{id}/seats` (cached in Redis, single-flight against stampedes).
2. **Waiting room** (flash-sale event only): `POST /events/{id}/queue/join`, then poll `GET .../queue/status` until `admitted: true`.
3. **Hold seats** (up to 4, 2 minutes): `POST /events/{id}/holds`. Of N racing buyers exactly one gets `201`; the rest get `409`.
4. **Check out:** `POST /orders` with a hold id and an `Idempotency-Key`. PENDING order -> charge -> seats `SOLD` + order `CONFIRMED` in one transaction.
   About 10% of simulated payments are declined on purpose; the seat is then released and the order is `FAILED`.
5. **Notify:** the order event goes through the outbox to Redis Streams and a consumer writes a notification row.

## Architecture (as deployed)

```
clients -> Render web service (FastAPI API + background loops in one process)
              |-> Neon Postgres: seats, holds, orders, outbox   <- source of truth
              |-> Render Key Value (Redis): rate limits, waiting room, cache, payment ledger, event stream
background loops (inside the web service): admit waiting-room users | release expired holds | outbox relay | notifier
```
Design decisions and failure modes: `docs/ARCHITECTURE.md`.

## Deploy (no Docker on your PC)

Full walkthrough: **`DEPLOY.md`**. In short:

1. **Neon:** create a project, copy the *pooled* and *direct* connection strings.
2. **GitHub:** put this folder in a new repository.
3. **Render:** New -> Blueprint -> choose the repo (it reads `render.yaml`), paste the two Neon strings, deploy.
4. Open `https://<your-service>.onrender.com/docs`.

Render builds the container from the included `Dockerfile` on its own servers. That file is for Render, not for your PC.

## Verify it works

Follow **`docs/RUNBOOK.pdf`**: 21 numbered steps, each with the PowerShell command and the expected result.
The short version:

1. `/health` returns `{"status":"ok"}`; `/events` lists 2 events; `/events/1/availability` shows 2000 available.
2. A hold on a seat returns `201`; the same seat for another user returns `409`.
3. An order returns `CONFIRMED` (or `FAILED` with `payment_declined`); repeating it returns the same `order_id`.
4. Event 2 holds return `403` until you are admitted from the waiting room.
5. `/admin/stats/1` shows all three invariants `true`.
6. The load test ends with `RESULT: PASS - zero oversells, ledger reconciles`.

Note: `/` and `/docs/health` return `Not Found` by design. Use `/docs` for the UI and `/health` at the root.

## API reference

| Endpoint | Purpose |
|---|---|
| `GET /health`, `GET /metrics` | Liveness; Prometheus metrics |
| `GET /events`, `GET /events/{id}` | Catalog (event 1 = open sale, event 2 = waiting room) |
| `GET /events/{id}/availability` | Available / held / sold counts (1 s cache) |
| `GET /events/{id}/seats?after=&limit=` | Free seats, keyset pagination |
| `POST /events/{id}/queue/join`, `GET /events/{id}/queue/status?ticket=` | Waiting room |
| `POST /events/{id}/holds`, `DELETE /holds/{id}` | Reserve / release seats |
| `POST /orders`, `GET /orders/{id}` | Checkout (idempotent) / order status |
| `GET /admin/stats/{id}` | Seat counts plus three correctness invariants |

Headers: `x-user-id` (required on most calls), `x-queue-ticket` (waiting-room events), `idempotency-key` (orders).

## Settings (Render environment variables)

`DATABASE_URL`, `DIRECT_DATABASE_URL` (Neon), `REDIS_URL` (auto-wired by the Blueprint), `RUN_WORKER_IN_API=1`, `AUTO_SEED=1`,
`DB_STATEMENT_CACHE=0`, `DB_POOL_MAX`, `REDIS_MAX_CONN`, `SEED_SEATS`. Optional tuning: `ADMIT_RATE`, `HOLD_TTL_SECONDS`,
`RATE_LIMIT_RPS`, `RATE_LIMIT_BURST`, `MAX_INFLIGHT`, `PAYMENT_DECLINE_RATE`, `PAYMENT_TIMEOUT_RATE`.

## Known limitations
- Auth is a trusted `X-User-Id` header (production: JWT validated at the gateway). `/admin/stats/*` is unauthenticated.
- The payment provider and email sending are simulated.
- Single Postgres node and a single API instance on the free tier. Next step: shard seats by event, run several API replicas.
- Free-tier hosting sleeps after ~15 minutes idle and is slow (0.1 CPU); treat the live link as a demo, not a benchmark.
- Free Render Key Value is 25 MB with no persistence: queue state and the payment ledger vanish on restart.
