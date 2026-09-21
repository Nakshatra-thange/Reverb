import asyncio
import random

async def handle_send_email(payload: dict):
    await asyncio.sleep(random.uniform(0.5, 2.0))  
    print(f"[email] sent to {payload.get('to')}")

async def handle_generate_report(payload: dict):
    await asyncio.sleep(random.uniform(1.0, 3.0))
    print(f"[report] generated for {payload.get('report_id')}")

HANDLERS = {
    "email": handle_send_email,
    "reports": handle_generate_report,
}

async def dispatch(queue: str, payload: dict):
    handler = HANDLERS.get(queue)
    if handler is None:
        raise ValueError(f"No handler registered for queue '{queue}'")
    await handler(payload)

_flaky_attempts = {}

async def handle_flaky_task(payload: dict):
    key = payload.get("id", "default")
    _flaky_attempts[key] = _flaky_attempts.get(key, 0) + 1
    if _flaky_attempts[key] < 3:
        raise RuntimeError(f"simulated transient failure (attempt {_flaky_attempts[key]})")
    print(f"[flaky] succeeded on attempt {_flaky_attempts[key]}")

HANDLERS["flaky"] = handle_flaky_task