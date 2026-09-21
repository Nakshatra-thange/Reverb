# Distributed Job Scheduler
## Architecture Decision Record

> **Scope:** Portfolio-scale, single-region, single-team job scheduling system  
> **Primary goal:** Reliable background execution with idempotency, crash recovery, retries, and observability.

---

## 1. Overview

### 1.1 Problem

Background tasks such as **emails, PDFs, webhooks, and retries** need to run reliably outside the request path:

- without duplicate side effects
- without silent loss on crash
- without one slow provider stalling everything else

### 1.2 Scale Assumptions

These assumptions are stated explicitly so every architectural decision can be checked against them:

| Dimension | Assumption |
|---|---|
| Sustained throughput | Low thousands of jobs/minute |
| Burst throughput | Roughly 10× sustained traffic |
| Job runtime | Sub-second to a few minutes |
| Deployment | Single-region |
| Ownership | Single team |
| Context | Portfolio-project traffic, not production SaaS traffic |

These assumptions are why **Redis Streams beats Kafka** here. If sustained throughput were 100× higher or multi-datacenter durability were required, several decisions below would change.

### 1.3 Non-Goals

The v1 system does **not** attempt to provide:

- Multi-tenant isolation
- Cross-region replication
- True exactly-once delivery
- A general workflow/DAG engine such as Temporal-style job chaining

Instead, the system uses **idempotent at-least-once execution**, which is the intended and defensible guarantee.

---

# 2. Technology Decisions

| Layer | Choice | Rejected Alternative | Why |
|---|---|---|---|
| API + workers | Python (FastAPI + plain worker processes) | Go | Faster to build correctly in 10 days; Go is a fine swap only if already fluent |
| Queue | Redis Streams | Kafka | Same core guarantees needed here at a fraction of the ops cost at this scale; swappable later behind a thin consumer interface |
| Database | PostgreSQL | MongoDB | Need real transactions and unique constraints for idempotency; relational fits the job/lease/DLQ shape naturally |
| Containerization | Docker Compose | Kubernetes | One person, one host — orchestration overhead exceeds the problem it solves at this scale |
| Observability | Prometheus + Grafana | Datadog / hosted SaaS | Free, self-hosted, no vendor signup, same signal quality at this scale |
| Load testing | k6 | Locust | Scriptable, lightweight, gives real throughput numbers to quote |
| Deployment | $5–6/mo VPS (Hetzner/DigitalOcean) | Managed cloud (ECS/GKE) | Whole stack runs comfortably on one small box; cost stays near zero |

> **Rule applied throughout:** Pick the boring, cheap option unless the scale assumptions force the expensive one.

---

# 3. Data Model

Three PostgreSQL tables carry most of the system.

## 3.1 `jobs`

| Column | Type | Notes |
|---|---|---|
| `id` | UUID PK | |
| `idempotency_key` | TEXT UNIQUE | Client-supplied; dedup happens here |
| `queue` | TEXT | Logical partition, e.g. `email`, `reports` |
| `payload` | JSONB | |
| `priority` | SMALLINT | Maps to stream tier |
| `status` | TEXT | `pending` / `running` / `succeeded` / `failed` / `dead` |
| `run_at` | TIMESTAMPTZ | Powers delayed jobs, cron, and retry backoff |
| `attempts` / `max_attempts` | INT | |
| `last_error` | TEXT | |

## 3.2 `job_leases`

Tracks active worker ownership:

```text
job_id
worker_id
leased_at
lease_expires_at
```

See [Crash Recovery](#7-crash-recovery).

## 3.3 `dead_letter_jobs`

Stores jobs that have exhausted their retry budget:

```text
job_id
payload
failure_history   -- JSONB array of attempt/error/timestamp
moved_at
```

## 3.4 Indexing Decisions

- **`(status, run_at)`** — powers the scheduler's "what's due" poll. Without it, that query becomes a full table scan as jobs accumulate.
- **`idempotency_key` as a UNIQUE constraint** — lets the database reject duplicate inserts atomically instead of relying on a check-then-insert race in application code.
- **`(queue, priority DESC, created_at)`** — only needed if priority is ever read back from PostgreSQL rather than purely from separate streams. Kept as a documented option, not built in v1.

---

# 4. Queue Design

## 4.1 Redis Streams

Use **one stream per priority tier per queue**.

Examples:

```text
jobs:email:high
jobs:email:default
jobs:email:low
```

Workers poll in this order:

```text
HIGH → DEFAULT → LOW
```

## 4.2 Consumer Groups

Each worker group uses `XREADGROUP` against a named consumer group.

Redis therefore tracks a **Pending Entries List (PEL)** per group.

The PEL is what makes crash recovery possible.

## 4.3 Delayed & Scheduled Jobs

Delayed and scheduled jobs use the same mechanism:

1. Scheduler polls PostgreSQL for `run_at <= now()`.
2. Due jobs are pushed onto the existing Redis Streams.
3. Workers consume them normally.

Retries use the exact same path:

```text
failure
   ↓
move run_at forward
   ↓
scheduler sees job when due
   ↓
push to Redis Stream
   ↓
worker retries
```

## 4.4 Priority Trade-off

Separate streams per tier are simple to reason about but can **starve low-priority jobs** under sustained high-priority load.

**Accepted for v1.**

A weighted round-robin alternative is documented in the trade-offs log if load testing shows starvation becoming a real problem.

---

# 5. Idempotency

There are **two distinct idempotency problems**, solved differently.

## 5.1 Producer-Side Idempotency

Scenario:

> Client retries the enqueue call after a timeout and doesn't know whether the original request landed.

Use:

```sql
INSERT ... 
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING id;
```

If no row is returned:

1. Look up the existing job.
2. Return its ID.

This gives one round trip and requires no separate application lock.

## 5.2 Consumer-Side Idempotency

Redis Streams provides **at-least-once**, not exactly-once delivery.

A worker can:

1. receive a job
2. finish the work
3. crash before acknowledging the message
4. another worker receives and executes it again

Therefore, handlers must be safe to run twice.

### Approach A — Idempotent by Design

Use when the side effect is naturally repeat-safe.

Example:

```sql
UPDATE ...
SET status = 'sent'
WHERE status != 'sent';
```

### Approach B — Idempotent Dedup Table

Use for side effects that are not naturally repeatable.

Example:

> Third-party payment call

Record that the execution happened in the same transaction as the side effect, and check the record before every attempt.

> **Best live demo:** Kill a worker mid-job and show that the side effect still happens exactly once.

---

# 6. Job Lifecycle

At a high level:

```text
Client
  │
  ▼
POST /jobs
  │
  ▼
PostgreSQL
  │
  │ run_at <= now()
  ▼
Scheduler
  │
  ▼
Redis Stream
  │
  ▼
Worker
  │
  ├── success ──► ACK ──► succeeded
  │
  └── failure
        │
        ▼
    backoff + jitter
        │
        ▼
      retry
        │
        └──────────────► max attempts
                              │
                              ▼
                             DLQ
```

---

# 7. Crash Recovery

## 7.1 Worker Lease

When a worker reads a job using `XREADGROUP`, the job enters the group's PEL.

The worker then creates a lease:

```text
lease_expires_at = now() + 30s
```

The lease is periodically renewed while the worker is genuinely still working.

## 7.2 Reaper

A separate reaper process:

1. Polls `XPENDING`.
2. Finds entries idle longer than the lease window.
3. Uses `XCLAIM` to transfer them to a live worker.
4. Increments the attempt count.

## 7.3 Dead-Letter Transition

If:

```text
attempts >= max_attempts
```

the job moves to:

```text
dead_letter_jobs
```

and is no longer reclaimed.

## 7.4 Lease Trade-off

Lease duration trades off:

| Shorter Lease | Longer Lease |
|---|---|
| Faster recovery | Slower recovery |
| Higher risk of false-positive reclaim | Lower risk of stealing a slow job |

**30 seconds** is the starting estimate based on expected job runtimes of a few minutes.

It should be tuned once real runtime data exists from load testing on Day 9.

---

# 8. Retry Policy

Use **exponential backoff with jitter**, rather than fixed delay.

```text
delay =
    min(base × 2^attempts, max_delay)
    + random(0, jitter)
```

### Why Jitter Matters

Without jitter, jobs that fail because of the same transient downstream problem can all retry at the exact same moment.

That creates a **thundering herd** against the service that just recovered.

## Retry Flow

```text
Job fails
   ↓
Calculate backoff + jitter
   ↓
Move run_at forward
   ↓
Scheduler polls due jobs
   ↓
Push job to Redis Stream
   ↓
Worker retries
```

After `max_attempts`, the job moves to the DLQ with its complete `failure_history`.

---

# 9. API Design & Authentication

## 9.1 API Endpoints

| Endpoint | Purpose |
|---|---|
| `POST /jobs` | Enqueue; requires an `Idempotency-Key` header |
| `GET /jobs/{id}` | Status lookup |
| `DELETE /jobs/{id}` | Cancel — only valid while `status = pending` |
| `GET /queues/{name}/stats` | Queue depth and oldest pending age |

The `Idempotency-Key` follows the same general pattern used by Stripe.

## 9.2 Authentication

Use **API keys scoped per client**, with rate limiting per key.

The existing rate limiter project plugs directly into the API as middleware.

JWT is intentionally not used:

> This is service-to-service traffic, not a user-facing session, so API keys are the right-sized and simpler choice.

---

# 10. Observability

Four signals matter more than a dashboard full of vanity graphs.

| Signal | What it tells us |
|---|---|
| **Queue depth per stream (`XLEN`)** | Earliest warning that consumers cannot keep up |
| **Job latency** | Enqueue-to-start and start-to-finish latency; track p50/p99 |
| **Retry rate + DLQ size** | Signals downstream problems before they become user-facing incidents |
| **Worker liveness** | Active leases vs. expected worker count |

> Use **histograms (p50/p99)** rather than averages because averages hide the tail that actually matters.

### Monitoring Stack

```text
API ──────────┐
              │
Workers ──────┼──► Prometheus ───► Grafana
              │
Redis ────────┘
```

Both Prometheus and Grafana run in the same Docker Compose setup.

---

# 11. Deployment Topology & Cost

## 11.1 Docker Compose

Everything runs in one `docker-compose.yml`:

```text
┌───────────────────────────────────────────┐
│                 VPS                        │
│                                           │
│  ┌───────┐  ┌───────┐  ┌──────────────┐ │
│  │ API   │  │ Redis │  │ PostgreSQL   │ │
│  └───────┘  └───────┘  └──────────────┘ │
│                                           │
│  ┌───────────┐  ┌────────────┐           │
│  │ Workers N │  │ Scheduler  │           │
│  └───────────┘  └────────────┘           │
│                                           │
│  ┌─────────────┐  ┌─────────┐             │
│  │ Prometheus  │  │ Grafana │             │
│  └─────────────┘  └─────────┘             │
└───────────────────────────────────────────┘
```

## 11.2 Cost

- **VPS:** approximately `$5–6/month` using Hetzner/DigitalOcean
- **Local demos:** free
- **No Kubernetes:** orchestration overhead exceeds the problem at this scale

This is worth stating explicitly in an interview rather than implying Kubernetes was never considered.

---

# 12. Failure Scenarios

| Failure | What Happens | Why It's Safe |
|---|---|---|
| **Redis down** | API returns `503` on enqueue; already-enqueued jobs pause until Redis returns | AOF persistence means existing stream data is not lost; Redis is the accepted single point of failure at this scale |
| **Worker crashes mid-job** | Lease expires → reaper reclaims → another worker retries | Lease mechanism + dedup table prevent double side effects |
| **PostgreSQL briefly unavailable** | Workers back off and retry the DB write; job remains in the PEL until ACK succeeds | At-least-once semantics tolerate this |
| **Downstream API down** | Jobs fail → backoff + jitter → eventually DLQ | Bounded retries prevent infinite retry storms |

---

# 13. Scaling Path Beyond Portfolio Scale

If the system grows beyond the stated assumptions:

### 13.1 Redis

Shard jobs by queue name across multiple Redis instances once one instance becomes the throughput bottleneck.

### 13.2 PostgreSQL

Partition the `jobs` table by `created_at` range once row count reaches the tens of millions.

Archive old completed jobs.

### 13.3 Workers

Workers already scale horizontally.

Redis consumer groups handle this without changing the business logic.

### 13.4 Kafka

Swap Redis Streams for Kafka if:

- retention needs move from hours to weeks, or
- multi-datacenter durability becomes a requirement

The consumer boundary is deliberately designed so this change does not touch business logic.

---

# 14. Open Trade-offs & Decision Log

| Decision | Alternative | Why This Way | Revisit If |
|---|---|---|---|
| Redis Streams over Kafka | Kafka | Lower ops cost at assumed scale | Sustained throughput or retention needs grow 10×+ |
| Separate priority streams | Weighted round-robin | Simpler to reason about and demo | Starvation becomes a real problem under load testing |
| API keys over JWT | JWT | Service-to-service traffic; no user session | A user-facing client is added |
| 30s lease duration | Shorter / longer | Starting estimate based on assumed job runtimes | After real runtime data from Day 9 load testing |
| Single Redis instance, no cluster | Redis Cluster | Unnecessary complexity at this scale | Redis itself becomes the throughput bottleneck |

---

# 15. Key Design Guarantees

The architecture is designed around a few explicit guarantees:

| Concern | Mechanism |
|---|---|
| Duplicate enqueue requests | PostgreSQL `UNIQUE(idempotency_key)` |
| Duplicate execution | Idempotent handlers / dedup table |
| Worker crash recovery | Redis PEL + leases + `XCLAIM` |
| Retry storms | Exponential backoff + jitter |
| Poison jobs | Maximum attempts + DLQ |
| Scheduled execution | PostgreSQL `run_at` + scheduler |
| Priority | Separate Redis Streams per tier |
| Queue visibility | Prometheus + Grafana |
| Horizontal workers | Redis consumer groups |
| Future queue migration | Thin consumer interface |

---

## 16. Interview-Level Summary

> **The core design choice is intentionally simple:** PostgreSQL is the source of truth for job state, Redis Streams handles durable asynchronous delivery, and workers use consumer groups plus leases for crash recovery. PostgreSQL unique constraints provide producer-side idempotency, while consumer-side idempotency is handled by job-specific safe operations or a dedup table. Retries use exponential backoff with jitter and eventually move to a DLQ. The whole system runs on Docker Compose on a small VPS because the stated portfolio-scale workload does not justify Kubernetes or Kafka. Every major choice has a documented scaling threshold at which it should be revisited.
