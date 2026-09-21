import json
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import FastAPI, Depends, Header, HTTPException
from pydantic import BaseModel

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