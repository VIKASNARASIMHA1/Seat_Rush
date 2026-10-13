"""Simulated payment provider. Idempotent by key; can decline, or 'time out' after charging."""
import asyncio, random, uuid
from . import config

class PaymentDeclined(Exception): pass
class PaymentTimeout(Exception): pass

async def charge(r, idem_key, amount_cents):
    await asyncio.sleep(random.uniform(0.05, 0.15))
    existing = await r.get(f"pay:idem:{idem_key}")
    if existing:
        return existing                      # provider-side dedupe: same key -> same charge
    if random.random() < config.PAYMENT_DECLINE_RATE:
        raise PaymentDeclined("card declined")
    ref = "pay_" + uuid.uuid4().hex[:16]
    await r.set(f"pay:idem:{idem_key}", ref, ex=86400)
    await r.hset("pay:charges", ref, amount_cents)
    if random.random() < config.PAYMENT_TIMEOUT_RATE:
        raise PaymentTimeout("response lost after charge")   # ambiguous outcome
    return ref

async def refund(r, ref):
    await r.hset("pay:refunds", ref, 1)

async def net_captured_cents(r):
    charges = await r.hgetall("pay:charges")
    refunds = await r.hgetall("pay:refunds")
    return sum(int(v) for k, v in charges.items() if k not in refunds)
