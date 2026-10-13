# Architecture Decision Records

**ADR-1 Postgres is the source of truth for inventory.** Redis-only locks lose data on failover and can't be joined to orders in one transaction.
Redis is used only for ephemeral/derivable state (queue, limits, cache).

**ADR-2 `FOR UPDATE SKIP LOCKED` holds vs. distributed locks / `SELECT ... FOR UPDATE`.** Plain row locks make losers wait (latency spikes, deadlocks on multi-seat
holds). SKIP LOCKED fails fast: a locked seat is "taken". Seats are processed in sorted order. No lock service to operate. Tradeoff: a loser may be told
"unavailable" for a seat whose winner later fails; the client simply retries.

**ADR-3 Holds expire lazily and eagerly.** The hold query treats expired holds as available (correctness never depends on the reaper); the reaper only cleans up.
Confirmation requires `hold_id` to still match, so a late payer loses and is refunded (compensation).

**ADR-4 Orchestrated saga, not 2PC.** Charge happens outside DB transactions. Failure after charge -> refund; failure before -> release seats.
The payment call is idempotent by key so a timeout retry cannot double-charge.

**ADR-5 Transactional outbox.** Order state and its event commit atomically; a relay publishes to a Redis Stream (at-least-once). Consumers are idempotent
(`UNIQUE(event_key)`), failures retry via XAUTOCLAIM and land in a DLQ after 3 attempts. Redis Streams chosen over Kafka for local simplicity; the interface is the same.

**ADR-6 Waiting room instead of just rate limiting.** Rate limits protect per caller; the waiting room protects the system by bounding concurrent buyers
and gives fairness (FIFO). Admission rate is the backpressure dial.

**ADR-7 Fail open on the limiter, fail fast on overload.** A dead Redis limiter must not cause an outage; an overloaded API returns 503 + Retry-After rather than queueing unboundedly.

**ADR-8 Short-TTL single-flight cache for read paths.** Availability counts are cached 1s with a lock so a thundering herd costs one DB query.

## Failure modes
| Failure | Behaviour |
|---|---|
| API instance restarts | Render restarts it; orders are idempotent so a client retry is safe |
| Worker dies | Queue stops admitting; holds still expire correctly (lazy takeover); outbox accumulates and drains on restart |
| Redis down | Rate limiter fails open; waiting-room holds rejected (403); orders fail at payment step and release seats |
| Payment timeout after charge | Retry with same key returns same charge; no double charge |
| Hold expires mid-payment | Seat may be taken over; confirm fails; charge refunded |
| Duplicate event delivery | Notification sink ignores duplicates |

## Capacity sketch (fill in with your measured numbers)
Hold = 1 short tx (~1-3 ms DB time) -> ~1-2k holds/s/Postgres core. Waiting-room admit rate should be set below that x average attempts per user.
Seat row ~100 B -> 50k seats ~ 5 MB (fits in memory); orders/outbox grow ~1 KB per order.
