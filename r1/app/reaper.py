import asyncio
import os
import uuid

from app.db import init_db_pool, close_db_pool, get_pool
from app.redis_stream import init_redis, close_redis, get_redis, push_job, stream_name
from app.lease_store import LEASE_TTL_SECONDS, release_lease

QUEUES = os.environ.get("QUEUES", "email,reports").split(",")
TIERS = [0, 1, 2]
GROUP = "workers"
REAPER_ID = "reaper"
POLL_INTERVAL_SECONDS = 5


async def reap_stream(stream: str):
    r = get_redis()
    pool = get_pool()

    next_id, claimed, _ = await r.xautoclaim(
        name=stream, groupname=GROUP, consumername=REAPER_ID,
        min_idle_time=LEASE_TTL_SECONDS * 1000, start_id="0-0", count=20,
    )

    for message_id, fields in claimed:
        job_id = fields["job_id"]
        print(f"[reaper] reclaiming job {job_id} from a dead worker (stream={stream})")

        # ack the stale message — we're about to re-enqueue a fresh one instead
        await r.xack(stream, GROUP, message_id)
        await release_lease(job_id, worker_id="")  # clear any stale lease row

        row = await pool.fetchrow(
            "SELECT attempts, max_attempts, queue, priority FROM jobs WHERE id = $1",
            uuid.UUID(job_id),
        )
        if row is None:
            continue

        if row["attempts"] >= row["max_attempts"]:
            await pool.execute(
                "UPDATE jobs SET status = 'dead', updated_at = now() WHERE id = $1",
                uuid.UUID(job_id),
            )
            print(f"[reaper] job {job_id} exhausted attempts on crash-reclaim, marking dead")
            continue

        await pool.execute(
            "UPDATE jobs SET status = 'pending', updated_at = now() WHERE id = $1",
            uuid.UUID(job_id),
        )
        await push_job(job_id, row["queue"], row["priority"])
        print(f"[reaper] job {job_id} requeued")


async def run_reaper():
    await init_db_pool()
    await init_redis()
    try:
        while True:
            for queue in QUEUES:
                for tier in TIERS:
                    await reap_stream(stream_name(queue, tier))
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
    finally:
        await close_db_pool()
        await close_redis()


if __name__ == "__main__":
    asyncio.run(run_reaper())