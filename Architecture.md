Distributed Job Scheduler — Architecture Decision Record


Overview
Problem. Background tasks (emails, PDFs, webhooks, retries) need to run reliably outside the request path, without duplicate side effects, without silent loss on crash, and without one slow provider stalling everything else.
Scale assumptions (stated explicitly so every decision below can be checked against them): low thousands of jobs/minute sustained, bursts to roughly 10x that; job runtimes from sub-second to a few minutes; single-region, single-team ownership; portfolio-project traffic, not production SaaS traffic. These assumptions are why Redis Streams beats Kafka here (see Tech stack) — if sustained throughput were 100x higher or multi-datacenter durability were required, several decisions below would flip.
Non-goals. Not building: multi-tenant isolation, cross-region replication, true exactly-once delivery (we build idempotent at-least-once instead, which is the honest and defensible answer), or a general workflow/DAG engine (Temporal-style job chaining is out of scope for v1).
Tech stack
Layer
Choice
Rejected alternative
Why
API + workers
Python (FastAPI + plain worker processes)
Go
Faster to build correctly in 10 days; Go is a fine swap only if already fluent
Queue
Redis Streams
Kafka
Same core guarantees needed here (durable log, consumer groups, pending-entry tracking) at a fraction of the ops cost at this scale; swappable later behind a thin consumer interface
Database
PostgreSQL
MongoDB
Need real transactions and unique constraints for idempotency; relational fits the job/lease/DLQ shape naturally
Containerization
Docker Compose
Kubernetes
One person, one host — orchestration overhead exceeds the problem it solves at this scale
Observability
Prometheus + Grafana
Datadog / hosted SaaS
Free, self-hosted, no vendor signup, same signal quality at this scale
Load testing
k6
Locust
Scriptable, lightweight, gives real throughput numbers to quote
Deployment
$5–6/mo VPS (Hetzner/DigitalOcean)
Managed cloud (ECS/GKE)
Whole stack runs comfortably on one small box; cost stays near zero
Rule applied throughout: pick the boring, cheap option unless the scale assumptions above force the expensive one.
Data model
Three Postgres tables carry most of the system.
jobs
Column
Type
Notes
id
UUID PK

idempotency_key
TEXT UNIQUE
client-supplied; dedup happens here
queue
TEXT
logical partition, e.g. email, reports
payload
JSONB

priority
SMALLINT
maps to stream tier, see Queue design
status
TEXT
pending / running / succeeded / failed / dead
run_at
TIMESTAMPTZ
powers delayed jobs, cron, and retry backoff
attempts / max_attempts
INT

last_error
TEXT

job_leases — job_id, worker_id, leased_at, lease_expires_at (see Crash recovery)
dead_letter_jobs — job_id, payload, failure_history (JSONB array of attempt/error/timestamp), moved_at
Indexing decisions
• (status, run_at) — powers the scheduler's "what's due" poll; without it that query becomes a full table scan as jobs accumulate.
• idempotency_key as a UNIQUE constraint, not just an index — lets the database reject a duplicate insert atomically instead of a check-then-insert race in application code.
• (queue, priority DESC, created_at) — only needed if priority is ever read back from Postgres rather than purely from separate streams; kept as a documented option, not built in v1 (see Trade-offs log).
Queue design
Redis Streams, one stream per priority tier per queue (e.g. jobs:email:high, jobs:email:default, jobs:email:low). Workers poll high before default before low.
Each worker group uses XREADGROUP against a named consumer group so Redis tracks a Pending Entries List (PEL) per group — this PEL is what makes crash recovery possible (see below). Delayed and scheduled jobs don't get a separate mechanism: the scheduler polls Postgres for run_at <= now() and pushes those jobs onto the same streams workers already read. Retries reuse this exact path too, by pushing run_at forward instead of retrying immediately.
Priority trade-off, stated up front: separate streams per tier is simple to reason about but can starve low-priority jobs under sustained high-priority load. Accepted for v1; a weighted round-robin alternative is noted in the Trade-offs log if this needs revisiting.
Idempotency
Two distinct problems, solved differently.
Producer-side (client retries the enqueue call after a timeout, unsure if it landed): INSERT ... ON CONFLICT (idempotency_key) DO NOTHING RETURNING id. No row returned → look up the existing job and return its id. One round trip, no separate lock needed.
Consumer-side (Redis Streams is at-least-once, not exactly-once — a worker can crash after finishing but before acking, so another worker re-runs the job): handlers must be safe to run twice. Two approaches, chosen per job type:
• Idempotent by design — the side effect is naturally repeat-safe (e.g. UPDATE ... SET status='sent' WHERE status != 'sent').
• Idempotent by dedup table — for side effects that aren't naturally repeatable (e.g. a third-party payment call): record "this execution happened" in the same transaction as the side effect, checked before every attempt.
This is the best live demo in the whole project: kill a worker mid-job, show it finishes exactly once anyway.
Crash recovery
1. A worker reads a job via XREADGROUP (adding it to the group's PEL) and writes a lease row in job_leases with lease_expires_at = now() + 30s, renewing it periodically while genuinely still working.
2. A separate reaper process polls XPENDING for entries idle longer than the lease window, XCLAIMs them for a live worker, and increments attempts.
3. If attempts >= max_attempts, the job moves to dead_letter_jobs instead of being reclaimed again.
Trade-off to defend: lease duration trades recovery latency against false-positive reclaims (stealing a job from a worker that's still alive but slow). 30s is a starting estimate based on expected job runtimes under a few minutes — worth tuning once real runtime data exists from load testing (Day 9).
Retry policy
Exponential backoff with jitter, not fixed delay: delay = min(base * 2^attempts, max_delay) + random(0, jitter). Jitter matters because without it, every job that failed due to a transient downstream blip retries at the exact same moment, creating a thundering herd against the service that just recovered.
Retries are implemented as the same mechanism as delayed jobs — push run_at forward and let the scheduler's due-job poll pick it up. No separate retry queue is needed.
After max_attempts, the job moves to dead_letter_jobs with its full failure_history, rather than retrying forever.
API design and authentication
Endpoint
Purpose
POST /jobs
Enqueue; requires an Idempotency-Key header (same pattern Stripe uses)
GET /jobs/{id}
Status lookup
DELETE /jobs/{id}
Cancel — only valid while status = pending
GET /queues/{name}/stats
Depth, oldest pending age
Auth: API keys scoped per client, rate-limited per key. The existing rate limiter project plugs in directly here as middleware — a deliberate composition, not a coincidence. JWT is intentionally not used: this is service-to-service traffic, not a user-facing session, so API keys are the right-sized choice and the simpler one to defend.
Observability
Four signals matter more than a dashboard full of vanity graphs:
• Queue depth per stream (XLEN) — earliest warning that consumers can't keep up.
• Job latency — enqueue-to-start and start-to-finish, as histograms (p50/p99), not averages, which hide the tail that actually matters.
• Retry rate and DLQ size — a spike here signals a downstream problem before it becomes a user-facing incident.
• Worker liveness — active leases vs. expected worker count.
Prometheus scrapes the API and workers; Grafana renders the dashboard. Both run in the same Docker Compose file as everything else.
Deployment topology and cost
One docker-compose.yml: Postgres, Redis, API, N worker containers, Prometheus, Grafana. Runs on a single $5–6/mo VPS, or free locally for demos. No Kubernetes: at this scale, orchestration overhead exceeds the problem it solves — worth stating explicitly in an interview rather than implying it wasn't considered.
Failure scenarios
Failure
What happens
Why it's safe
Redis down
API returns 503 on enqueue; already-enqueued jobs pause until Redis returns
AOF persistence means nothing already in the stream is lost; Redis is the accepted single point of failure at this scale
Worker crashes mid-job
Lease expires, reaper reclaims, another worker retries
Lease mechanism + dedup table prevent double side effects
Postgres briefly unavailable
Workers back off and retry the DB write; job stays in the PEL until ack succeeds
At-least-once semantics tolerate this
Downstream API down
Jobs fail, backoff+jitter spaces retries, eventually hit DLQ
Bounded retries prevent infinite retry storms
Scaling path beyond portfolio scale
• Shard jobs by queue name across multiple Redis instances once one instance's throughput becomes the bottleneck.
• Partition the jobs table by created_at range once row count reaches the tens of millions; archive old completed jobs out.
• Workers already scale horizontally for free — consumer groups handle that.
• Swap Redis Streams for Kafka if retention needs move from hours to weeks, or multi-datacenter durability becomes a requirement — the consumer boundary is designed so this doesn't touch business logic.
Open trade-offs and decisions log
Decision
Alternative considered
Why this way
Revisit if
Redis Streams over Kafka
Kafka
Lower ops cost at assumed scale
Sustained throughput or retention needs grow 10x+
Separate priority streams
Weighted round-robin
Simpler to reason about and demo
Starvation becomes a real problem under load testing
API keys over JWT
JWT
Service-to-service traffic, no user session
A user-facing client is added
30s lease duration
Shorter/longer
Starting estimate based on assumed job runtimes
After real runtime data from load testing (Day 9)
Single Redis instance, no cluster
Redis Cluster
Unnecessary complexity at this scale
Redis itself becomes the throughput bottleneck