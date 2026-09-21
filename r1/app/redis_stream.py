import redis.asyncio as redis
import os

_redis: redis.Redis | None = None

PRIORITY_TIER = {0: "high", 1: "default", 2: "low"}

def stream_name(queue: str, priority: int) -> str:
    return f"jobs:{queue}:{PRIORITY_TIER.get(priority, 'default')}"

async def init_redis():
    global _redis
    _redis = redis.from_url(os.environ["REDIS_URL"], decode_responses=True)

async def close_redis():
    if _redis:
        await _redis.aclose()

def get_redis() -> redis.Redis:
    if _redis is None:
        raise RuntimeError("Redis not initialized")
    return _redis

async def push_job(job_id: str, queue: str, priority: int):
    r = get_redis()
    stream = stream_name(queue, priority)
    await r.xadd(stream, {"job_id": job_id})

async def stream_depth(queue: str) -> dict:
    r = get_redis()
    depths = {}
    for tier in PRIORITY_TIER.values():
        depths[tier] = await r.xlen(f"jobs:{queue}:{tier}")
    return depths
async def ensure_consumer_group(stream: str, group: str):
    r = get_redis()
    try:
        await r.xgroup_create(name=stream, groupname=group, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise  # group already exists — fine, ignore

async def read_group(stream: str, group: str, consumer: str, count: int = 5, block_ms: int = 2000):
    r = get_redis()
    return await r.xreadgroup(
        groupname=group, consumername=consumer,
        streams={stream: ">"}, count=count, block=block_ms,
    )

async def ack(stream: str, group: str, message_id: str):
    r = get_redis()
    await r.xack(stream, group, message_id)