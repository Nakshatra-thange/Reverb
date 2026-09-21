import json
import uuid
from app.db import get_pool

async def move_to_dead_letter(job_id: str, payload, error: str):
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO dead_letter_jobs (job_id, payload, failure_history)
                VALUES ($1, $2, $3::jsonb)
                ON CONFLICT (job_id) DO UPDATE
                  SET failure_history = dead_letter_jobs.failure_history || $3::jsonb,
                      moved_at = now()
                """,
                uuid.UUID(job_id),
                json.dumps(payload) if not isinstance(payload, str) else payload,
                json.dumps([{"error": error}]),
            )
            await conn.execute(
                "UPDATE jobs SET status = 'dead', updated_at = now() WHERE id = $1",
                uuid.UUID(job_id),
            )