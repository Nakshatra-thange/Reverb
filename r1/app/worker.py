import asyncio
import json
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

import time
from app.metrics import (
    jobs_processed_total, job_duration_seconds, job_wait_seconds,
    retries_total, dlq_moves_total, start_metrics_server,
)
from app.db import init_db_pool, close_db_pool, get_pool
from app.redis_stream import (
    init_redis, close_redis, ensure_consumer_group, read_group, ack, stream_name,
)
from app.job_handlers import dispatch
from app.lease_store import acquire_lease, renew_lease, release_lease, LEASE_TTL_SECONDS
from app.backoff import compute_backoff_seconds
from app.dlq import move_to_dead_letter

GROUP = "workers"
QUEUE = "email"
WORKER_ID = f"worker-{uuid.uuid4().hex[:8]}"
TIERS = ["high", "default", "low"]


async def fetch_job(job_id: str):
    pool = get_pool()
    return await pool.fetchrow(
        "SELECT id, queue, payload, status, priority, attempts, max_attempts FROM jobs WHERE id = $1",
        uuid.UUID(job_id),
    )


async def mark_running(job_id: str):
    pool = get_pool()
    row = await pool.fetchrow(
        "UPDATE jobs SET status='running', attempts = attempts + 1, updated_at = now() WHERE id = $1 RETURNING attempts",
        uuid.UUID(job_id),
    )
    return row["attempts"]


async def mark_succeeded(job_id: str):
    pool = get_pool()
    await pool.execute("UPDATE jobs SET status='succeeded', updated_at = now() WHERE id = $1", uuid.UUID(job_id))


async def schedule_retry(job_id: str, attempts: int, error: str):
    pool = get_pool()
    delay = compute_backoff_seconds(attempts)
    next_run_at = datetime.now(timezone.utc) + timedelta(seconds=delay)
    await pool.execute(
        "UPDATE jobs SET status='pending', run_at=$2, last_error=$3, updated_at=now() WHERE id = $1",
        uuid.UUID(job_id), next_run_at, error,
    )
    print(f"[{WORKER_ID}] job {job_id} failed (attempt {attempts}), retry in {delay:.1f}s: {error}")


async def keep_lease_alive(job_id: str, worker_id: str, stop_event: asyncio.Event):
    """Background task: renew the lease at half the TTL interval while the handler runs."""
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=LEASE_TTL_SECONDS / 2)
        except asyncio.TimeoutError:
            await renew_lease(job_id, worker_id)

async def process_message(stream: str, message_id: str, fields: dict):
    job_id = fields["job_id"]
    job = await fetch_job(job_id)

    if job is None:
        await ack(stream, GROUP, message_id)
        return

    if job["status"] in ("succeeded", "cancelled", "dead"):
        await ack(stream, GROUP, message_id)
        return

    if not await acquire_lease(job_id, WORKER_ID):
        await ack(stream, GROUP, message_id)
        return

    # Redis stream message ids are "<ms-timestamp>-<seq>" — free enqueue-to-pickup latency
    enqueue_ms = int(message_id.split("-")[0])
    job_wait_seconds.labels(queue=job["queue"]).observe((time.time() * 1000 - enqueue_ms) / 1000)

    stop_event = asyncio.Event()
    lease_task = asyncio.create_task(keep_lease_alive(job_id, WORKER_ID, stop_event))
    start = time.monotonic()

    try:
        attempts = await mark_running(job_id)
        payload = json.loads(job["payload"]) if isinstance(job["payload"], str) else job["payload"]
        await dispatch(job["queue"], payload)
        await mark_succeeded(job_id)
        job_duration_seconds.labels(queue=job["queue"]).observe(time.monotonic() - start)
        jobs_processed_total.labels(queue=job["queue"], outcome="succeeded").inc()
        print(f"[{WORKER_ID}] job {job_id} succeeded")

    except Exception as e:
        job_duration_seconds.labels(queue=job["queue"]).observe(time.monotonic() - start)
        if attempts >= job["max_attempts"]:
            await move_to_dead_letter(job_id, job["payload"], str(e))
            dlq_moves_total.labels(queue=job["queue"]).inc()
            jobs_processed_total.labels(queue=job["queue"], outcome="dead").inc()
            print(f"[{WORKER_ID}] job {job_id} exhausted retries, moved to DLQ: {e}")
        else:
            await schedule_retry(job_id, attempts, str(e))
            retries_total.labels(queue=job["queue"]).inc()
            jobs_processed_total.labels(queue=job["queue"], outcome="failed").inc()

    finally:
        stop_event.set()
        await lease_task
        await release_lease(job_id, WORKER_ID)
        await ack(stream, GROUP, message_id)


async def poll_tier(tier: str):
    stream = f"jobs:{QUEUE}:{tier}"
    await ensure_consumer_group(stream, GROUP)
    result = await read_group(stream, GROUP, WORKER_ID, count=5, block_ms=500)
    if not result:
        return False
    for _stream_name, messages in result:
        for message_id, fields in messages:
            await process_message(stream, message_id, fields)
    return True


async def run_worker():
    metrics_port = int(os.environ.get("METRICS_PORT", 9100))
    start_metrics_server(metrics_port)
    await init_db_pool()
    await init_redis()
    print(f"[{WORKER_ID}] starting, serving queue '{QUEUE}', metrics on :{metrics_port}")
   
    try:
        while True:
            found_work = False
            for tier in TIERS:
                if await poll_tier(tier):
                    found_work = True
                    break
            if not found_work:
                await asyncio.sleep(0.5)
    finally:
        await close_db_pool()
        await close_redis()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        QUEUE = sys.argv[1]
    asyncio.run(run_worker())