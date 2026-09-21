import asyncio
import json
import uuid
from datetime import datetime, timezone

from croniter import croniter

from app.db import init_db_pool, close_db_pool, get_pool
from app.redis_stream import init_redis, close_redis, push_job

POLL_INTERVAL_SECONDS = 2


async def dispatch_due_jobs():
    """Covers three cases with one query: brand-new delayed jobs whose run_at
    has arrived, and retries whose backoff has elapsed. Both just look like
    'pending, run_at <= now()' rows — that's the whole point of reusing this path."""
    pool = get_pool()
    rows = await pool.fetch(
        """
        SELECT id, queue, priority FROM jobs
        WHERE status = 'pending' AND run_at <= now()
        LIMIT 100
        """
    )
    for row in rows:
        await push_job(str(row["id"]), row["queue"], row["priority"])
        print(f"[scheduler] dispatched due job {row['id']} (queue={row['queue']})")


async def materialize_recurring_jobs():
    """Finds cron schedules that are due, creates a real job row for this
    occurrence, pushes it, and advances next_run_at using croniter."""
    pool = get_pool()
    rows = await pool.fetch(
        """
        SELECT id, name, queue, payload, priority, cron_expression, next_run_at
        FROM recurring_jobs
        WHERE enabled = true AND next_run_at <= now()
        LIMIT 50
        """
    )
    for row in rows:
        job_id = uuid.uuid4()
        idempotency_key = f"recurring:{row['name']}:{row['next_run_at'].isoformat()}"

        async with pool.acquire() as conn:
            async with conn.transaction():
                inserted = await conn.fetchrow(
                    """
                    INSERT INTO jobs (id, idempotency_key, queue, payload, priority, status, run_at)
                    VALUES ($1, $2, $3, $4, $5, 'pending', now())
                    ON CONFLICT (idempotency_key) DO NOTHING
                    RETURNING id
                    """,
                    job_id, idempotency_key, row["queue"], row["payload"], row["priority"],
                )

                base = row["next_run_at"]
                next_run = croniter(row["cron_expression"], base).get_next(datetime)
                if next_run.tzinfo is None:
                    next_run = next_run.replace(tzinfo=timezone.utc)

                await conn.execute(
                    "UPDATE recurring_jobs SET last_run_at = now(), next_run_at = $2 WHERE id = $1",
                    row["id"], next_run,
                )

        if inserted:
            await push_job(str(job_id), row["queue"], row["priority"])
            print(f"[scheduler] fired recurring job '{row['name']}' -> {job_id}, next run {next_run}")


async def run_scheduler():
    await init_db_pool()
    await init_redis()
    try:
        while True:
            await dispatch_due_jobs()
            await materialize_recurring_jobs()
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
    finally:
        await close_db_pool()
        await close_redis()


if __name__ == "__main__":
    asyncio.run(run_scheduler())