import asyncio
import time
import httpx

API = "http://localhost:8000"
HEADERS = {"X-API-Key": "dev-key-123", "Content-Type": "application/json"}


async def enqueue(client: httpx.AsyncClient, i: int, priority: int):
    key = f"load-{priority}-{i}-{time.time_ns()}"
    payload = {"queue": "email", "payload": {"to": f"user{i}@x.com"}, "priority": priority}
    resp = await client.post(
        f"{API}/jobs", json=payload,
        headers={**HEADERS, "Idempotency-Key": key},
    )
    return resp.json()["id"]


async def flood(n_high: int, n_low: int):
    async with httpx.AsyncClient(timeout=10) as client:
        # enqueue a big batch of low-priority first, then a smaller batch of high-priority
        low_ids = await asyncio.gather(*[enqueue(client, i, priority=2) for i in range(n_low)])
        print(f"enqueued {n_low} low-priority jobs")

        await asyncio.sleep(1)

        high_ids = await asyncio.gather(*[enqueue(client, i, priority=0) for i in range(n_high)])
        print(f"enqueued {n_high} high-priority jobs")

        return low_ids, high_ids


async def poll_until_done(client: httpx.AsyncClient, job_id: str) -> float:
    start = time.time()
    while True:
        resp = await client.get(f"{API}/jobs/{job_id}", headers=HEADERS)
        if resp.json()["status"] in ("succeeded", "failed", "dead"):
            return time.time() - start
        await asyncio.sleep(0.5)


async def main():
    low_ids, high_ids = await flood(n_high=20, n_low=200)

    async with httpx.AsyncClient(timeout=120) as client:
        high_latencies = await asyncio.gather(*[poll_until_done(client, i) for i in high_ids])
        low_latencies = await asyncio.gather(*[poll_until_done(client, i) for i in low_ids])

    print(f"\nhigh-priority avg completion time: {sum(high_latencies)/len(high_latencies):.2f}s")
    print(f"low-priority avg completion time:  {sum(low_latencies)/len(low_latencies):.2f}s")


if __name__ == "__main__":
    asyncio.run(main())