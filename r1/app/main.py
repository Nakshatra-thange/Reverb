import json
from contextlib import asynccontextmanager
from uuid import UUID
from datetime import datetime, timedelta, timezone
from croniter import croniter

from fastapi import FastAPI, Depends, Header, HTTPException
from pydantic import BaseModel
from app.metrics import jobs_enqueued_total, jobs_deduped_total, queue_depth
from prometheus_client import make_asgi_app
from app.db import init_db_pool, close_db_pool, get_pool
from app.redis_stream import init_redis, close_redis, push_job, stream_depth
from app.auth import require_api_key


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db_pool()
    await init_redis()
    yield
    await close_db_pool()
    await close_redis()


app = FastAPI(lifespan=lifespan)
app.mount("/metrics", make_asgi_app())


class JobCreate(BaseModel):
    queue: str
    payload: dict
    priority: int = 1  # 0=high, 1=default, 2=low


@app.post("/jobs", status_code=201)
async def create_job(
    body: JobCreate,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    api_key: str = Depends(require_api_key),
):
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO jobs (idempotency_key, queue, payload, priority)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING id, status
            """,
            idempotency_key, body.queue, json.dumps(body.payload), body.priority,
        )

        if row is None:
            # already exists — return the existing job instead of erroring
            existing = await conn.fetchrow(
                "SELECT id, status FROM jobs WHERE idempotency_key = $1", idempotency_key
            )
            return {"id": str(existing["id"]), "status": existing["status"], "deduped": True}

        await push_job(str(row["id"]), body.queue, body.priority)
        return {"id": str(row["id"]), "status": row["status"], "deduped": False}


@app.get("/jobs/{job_id}")
async def get_job(job_id: UUID, api_key: str = Depends(require_api_key)):
    pool = get_pool()
    row = await pool.fetchrow(
        """SELECT id, queue, status, priority, attempts, max_attempts,
                  last_error, run_at, created_at, updated_at
           FROM jobs WHERE id = $1""",
        job_id,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return dict(row)


@app.delete("/jobs/{job_id}")
async def cancel_job(job_id: UUID, api_key: str = Depends(require_api_key)):
    pool = get_pool()
    result = await pool.execute(
        "UPDATE jobs SET status = 'cancelled', updated_at = now() WHERE id = $1 AND status = 'pending'",
        job_id,
    )
    if result == "UPDATE 0":
        raise HTTPException(status_code=409, detail="Job not cancellable (already running or missing)")
    return {"id": str(job_id), "status": "cancelled"}


@app.get("/queues/{queue}/stats")
async def queue_stats(queue: str, api_key: str = Depends(require_api_key)):
    pool = get_pool()
    counts = await pool.fetch(
        "SELECT status, count(*) FROM jobs WHERE queue = $1 GROUP BY status", queue
    )
    depths = await stream_depth(queue)
    return {
        "queue": queue,
        "postgres_status_counts": {r["status"]: r["count"] for r in counts},
        "redis_stream_depth": depths,
    }
class JobCreate(BaseModel):
    queue: str
    payload: dict
    priority: int = 1
    max_attempts: int = 5
    delay_seconds: int = 0       # 0 = run now, otherwise delay before first execution


@app.post("/jobs", status_code=201)
async def create_job(
    body: JobCreate,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    api_key: str = Depends(require_api_key),
):
    pool = get_pool()
    run_at = datetime.now(timezone.utc) + timedelta(seconds=body.delay_seconds)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO jobs (idempotency_key, queue, payload, priority, max_attempts, run_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING id, status
            """,
            idempotency_key, body.queue, json.dumps(body.payload),
            body.priority, body.max_attempts, run_at,
        )

        if row is None:
            existing = await conn.fetchrow(
                "SELECT id, status FROM jobs WHERE idempotency_key = $1", idempotency_key
            )
            return {"id": str(existing["id"]), "status": existing["status"], "deduped": True}

        if body.delay_seconds == 0:
            await push_job(str(row["id"]), body.queue, body.priority)
        # else: leave it for the scheduler's due-job poll to pick up at run_at

        return {"id": str(row["id"]), "status": row["status"], "deduped": False, "run_at": run_at.isoformat()}


class RecurringJobCreate(BaseModel):
    name: str
    queue: str
    payload: dict
    priority: int = 1
    cron_expression: str   # e.g. "*/5 * * * *" = every 5 minutes


@app.post("/schedules", status_code=201)
async def create_schedule(body: RecurringJobCreate, api_key: str = Depends(require_api_key)):
    if not croniter.is_valid(body.cron_expression):
        raise HTTPException(status_code=400, detail="Invalid cron expression")

    first_run = croniter(body.cron_expression, datetime.now(timezone.utc)).get_next(datetime)
    pool = get_pool()
    row = await pool.fetchrow(
        """
        INSERT INTO recurring_jobs (name, queue, payload, priority, cron_expression, next_run_at)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (name) DO NOTHING
        RETURNING id, next_run_at
        """,
        body.name, body.queue, json.dumps(body.payload), body.priority, body.cron_expression, first_run,
    )
    if row is None:
        raise HTTPException(status_code=409, detail="Schedule with this name already exists")
    return {"id": str(row["id"]), "next_run_at": row["next_run_at"].isoformat()}


@app.get("/schedules")
async def list_schedules(api_key: str = Depends(require_api_key)):
    pool = get_pool()
    rows = await pool.fetch("SELECT id, name, queue, cron_expression, next_run_at, last_run_at, enabled FROM recurring_jobs")
    return [dict(r) for r in rows]


@app.delete("/schedules/{schedule_id}")
async def disable_schedule(schedule_id: UUID, api_key: str = Depends(require_api_key)):
    pool = get_pool()
    await pool.execute("UPDATE recurring_jobs SET enabled = false WHERE id = $1", schedule_id)
    return {"id": str(schedule_id), "enabled": False}