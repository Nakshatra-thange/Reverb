import asyncpg
import os

_pool: asyncpg.Pool | None = None

async def init_db_pool():
    global _pool
    _pool = await asyncpg.create_pool(
        dsn=os.environ["DATABASE_URL"],
        min_size=2,
        max_size=10,
        statement_cache_size=0,   # required behind Neon's PgBouncer pooler
    )

async def close_db_pool():
    if _pool:
        await _pool.close()

def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB pool not initialized")
    return _pool