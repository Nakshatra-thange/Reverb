import os
import uuid
from datetime import datetime, timedelta, timezone

from app.db import get_pool

LEASE_TTL_SECONDS = int(os.environ.get("LEASE_TTL_SECONDS", 30))


async def acquire_lease(job_id: str, worker_id: str) -> bool:
    """Claim a lease for this job. Succeeds if unleased, or if the
    previous lease already expired (previous worker likely crashed)."""
    pool = get_pool()
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=LEASE_TTL_SECONDS)
    row = await pool.fetchrow(
        """
        INSERT INTO job_leases (job_id, worker_id, leased_at, lease_expires_at)
        VALUES ($1, $2, now(), $3)
        ON CONFLICT (job_id) DO UPDATE
          SET worker_id = EXCLUDED.worker_id,
              leased_at = now(),
              lease_expires_at = EXCLUDED.lease_expires_at
          WHERE job_leases.lease_expires_at < now()
        RETURNING job_id
        """,
        uuid.UUID(job_id), worker_id, expires_at,
    )
    return row is not None


async def renew_lease(job_id: str, worker_id: str):
    pool = get_pool()
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=LEASE_TTL_SECONDS)
    await pool.execute(
        "UPDATE job_leases SET lease_expires_at = $3 WHERE job_id = $1 AND worker_id = $2",
        uuid.UUID(job_id), worker_id, expires_at,
    )


async def release_lease(job_id: str, worker_id: str):
    pool = get_pool()
    await pool.execute(
        "DELETE FROM job_leases WHERE job_id = $1 AND worker_id = $2",
        uuid.UUID(job_id), worker_id,
    )