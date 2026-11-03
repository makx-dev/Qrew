# Architectural Decision Records (ADR) & System Design Document

**Project:** Qrew (Distributed Asynchronous Job Queue Engine)  
**Status:** Living Document  
**Last Updated:** 2026-10-31  
**Primary Tech Stack:** Node.js, TypeScript, BullMQ, Redis (`ioredis`), Express, Docker  

---

## Table of Contents
1. [System Architecture & End-to-End Data Flow](#1-system-architecture--end-to-end-data-flow)
2. [Storage & Broker Evaluation: Redis + BullMQ](#2-storage--broker-evaluation-redis--bullmq)
3. [Architectural Decision Records (ADRs)](#3-architectural-decision-records-adrs)
   - [ADR-001: Broker & Queue Abstraction Engine](#adr-001-broker--queue-abstraction-engine)
   - [ADR-002: Strict Priority Scheduling via Sorted Sets (`ZSET`)](#adr-002-strict-priority-scheduling-via-sorted-sets-zset)
   - [ADR-003: Tiered Retry Backoff with Jitter Tailored by Job SLA](#adr-003-tiered-retry-backoff-with-jitter-tailored-by-job-sla)
   - [ADR-004: Terminal vs. Transient Error Handling (`UnrecoverableError`)](#adr-004-terminal-vs-transient-error-handling-unrecoverableerror)
   - [ADR-005: Redis Connection & `maxRetriesPerRequest: null`](#adr-005-redis-connection--maxretriesperrequest-null)
   - [ADR-006: Deterministic Two-Phase Graceful Shutdown](#adr-006-deterministic-two-phase-graceful-shutdown)
   - [ADR-007: Job Retention & Memory Pruning](#adr-007-job-retention--memory-pruning)
   - [ADR-008: In-Worker Lifecycle Telemetry](#adr-008-in-worker-lifecycle-telemetry)
   - [ADR-009: Empirical Jitter Verification Harness](#adr-009-empirical-jitter-verification-harness)
   - [ADR-010: Native Dead-Letter Architecture & Redlock Replay](#adr-010-native-dead-letter-architecture--redlock-replay)
   - [ADR-011: Edge-Decoupled REST API Ingress & SHA-256 Idempotency](#adr-011-edge-decoupled-rest-api-ingress--sha-256-idempotency)
   - [ADR-012: In-Memory Throttled Progress Telemetry & On-Demand Polling](#adr-012-in-memory-throttled-progress-telemetry--on-demand-polling)
   - [ADR-013: Multi-Role Observability, Bull Board UI Partitioning & Fail-Safe RBAC](#adr-013-multi-role-observability-bull-board-ui-partitioning--fail-safe-rbac)
   - [ADR-014: Decoupled CacheCut Click-Sync Consumer Engine & Scheduler Pruning](#adr-014-decoupled-cachecut-click-sync-consumer-engine--scheduler-pruning)
4. [Failure Modes & Mitigation Matrix](#4-failure-modes--mitigation-matrix)
5. [Load Testing & Invariant Verification](#5-load-testing--invariant-verification)
6. [Scaling Roadmap (100x Scale)](#6-scaling-roadmap-100x-scale)

---

## 1. System Architecture & End-to-End Data Flow

Qrew decouples synchronous API interactions from compute-intensive and rate-limited downstream workflows (transactional OTPs, bulk emails, and image processing).

```
  [ HTTP Client / Microservices ]
                 │
                 ├── POST /api/v1/jobs (Idempotency-Key + JSON Payload)
                 ▼
  ┌────────────────────────────────────────────────────────┐
  │         REST API Ingress Layer (Express)               │
  │  - Payload Schema Validation (Fast 400 rejection)      │
  │  - SHA-256 Canonical Fingerprint & Idempotency Cache   │
  │  - Returns 202 Accepted + Location Header              │
  └──────────────────────────┬─────────────────────────────┘
                             │
                             ▼
  ┌────────────────────────────────────────────────────────┐
  │                 REDIS BROKER (BullMQ)                  │
  │  bull:my-queue:prioritized  --> ZSET [Score = Priority]│
  │  bull:my-queue:delayed      --> ZSET [Score = RunAt]   │
  │  bull:my-queue:active       --> List (In-Flight)       │
  │  bull:my-queue:failed       --> Hash (DLQ, Capped 1000)│
  └──────────────────────────┬─────────────────────────────┘
                             │
                             ▼
  ┌────────────────────────────────────────────────────────┐
  │             WORKER LAYER (Concurrency: 1)              │
  │  - Auto Heartbeat & Lock Extension (prevents stalls)   │
  │  - P1: OTP (200ms, Exponential Backoff + 20% Jitter)   │
  │  - P2: Email (500ms, Exp Backoff + 50% Jitter for 429) │
  │  - P3: Image (1000ms, Throttled Progress + 30% Jitter) │
  │  - Terminal Errors --> Immediate PoisonPill Quarantine │
  └────────────────────────────────────────────────────────┘
```

### State Lifecycle
1. **Ingest**: API validates payload, claims idempotency key, enqueues to Redis `prioritized` ZSET via atomic Lua script, returns `202 Accepted`.
2. **Dequeue & Lock**: Worker atomically shifts highest-priority job from `prioritized` to `active` and acquires an exclusive lock token.
3. **Execution & Heartbeat**: Worker renews lock periodically. If worker crashes, stalled-job monitor returns the task to `prioritized`.
4. **Resolution**:
   - *Success*: Moved to `completed` (capped at 1,000 records).
   - *Transient Error*: Scheduled into `delayed` ZSET with exponential backoff + jitter.
   - *Terminal Error / Poison Pill*: Short-circuits remaining retries via `UnrecoverableError`, landing in `failed` (DLQ).

---

## 2. Storage & Broker Evaluation: Redis + BullMQ

| Metric | **Redis + BullMQ (Selected)** | **Apache Kafka** | **RabbitMQ** | **AWS SQS** | **PostgreSQL (SKIP LOCKED)** |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Latency** | **< 1ms (Sub-millisecond)** | 5–20ms | 1–5ms | 20–100ms | 5–50ms |
| **Priority** | **Native O(log N) via `ZSET`** | Anti-pattern (multi-topic) | Heavy max-priority queues | Not supported natively | Index polling (`ORDER BY`) |
| **Delay/Schedule** | **Native O(log N) delayed `ZSET`**| External scheduler needed | DLX + TTL dead-letter plugins| Up to 15 min max | `run_at <= NOW()` polling |
| **Throughput** | **50,000–100,000 ops/sec** | 100,000+ ops/sec | 20,000–40,000 ops/sec | Managed scale | 2,000–5,000 ops/sec |
| **Dev Parity** | Single binary (`docker run`) | High (ZooKeeper/KRaft) | Erlang runtime & vhosts | Cloud-locked (local mocks) | Low if DB exists |

**Why Redis Won:** Real-time OTPs demand sub-5ms roundtrips. Redis Sorted Sets provide native O(log N) dynamic priority and arbitrary retry delays without external schedulers, orchestrated atomically via Lua scripts.

---

## 3. Architectural Decision Records (ADRs)

### ADR-001: Broker & Queue Abstraction Engine
* **Status:** Accepted | **Date:** 2026-10-19
* **Context:** Needed a high-throughput, low-latency task engine supporting dynamic priority, arbitrary delays, and TypeScript types.
* **Decision:** Selected **BullMQ + Redis (`ioredis`)**. Strongly typed job contracts (`JobTypes.ts`) shared between producer and worker.
* **Alternatives Considered:**
  - *Apache Kafka:* High operational complexity, no native priority or arbitrary delay scheduling without external orchestrators.
  - *RabbitMQ (AMQP):* Erlang dependency, complex partition recovery, high memory footprint on deep queues.
  - *AWS SQS:* 20–100ms HTTP polling latency, poor local dev parity, vendor lock-in.
  - *PostgreSQL (`SKIP LOCKED`):* Table bloat from write churn, WAL write overhead, lock contention capping throughput at 2,000–5,000 ops/sec.
* **Trade-off:** High throughput and low latency; requires Redis RAM monitoring and `noeviction` policy to prevent data loss.

---

### ADR-002: Strict Priority Scheduling via Sorted Sets (`ZSET`)
* **Status:** Accepted | **Date:** 2026-10-21
* **Context:** Critical OTPs must never be starved behind bulk image processing or marketing emails.
* **Decision:** Defined numerical priority enum (`CRITICAL = 1`, `HIGH = 2`, `MEDIUM = 3`). BullMQ maps priority to Redis `ZSET` scores (`(priority << 40) | seq`), ensuring lower numerical scores dequeue first in $O(\log N)$ time.
* **Alternatives Considered:**
  - *Dedicated FIFO Queues per Priority:* Polling `queue:critical`, then `high`, then `medium` increases connection and queue proliferation; deferred to multi-worker scaling phase.
  - *In-Memory Min-Heap (Node.js Process):* State lost on process crash; cannot coordinate across distributed worker instances.
  - *Deficit Weighted Round Robin (e.g. 3:2:1 ratio):* Statistical rather than deterministic; critical OTPs demand absolute preemption.
* **Trade-off:** Critical jobs preempt lower tiers instantly; requires multi-queue dedicated worker pools if critical traffic floods continuously.

---

### ADR-003: Tiered Retry Backoff with Jitter Tailored by Job SLA
* **Status:** Accepted | **Date:** 2026-10-23
* **Context:** Naive uniform retries caused downstream thundering herd collisions and rate limit (HTTP 429) cascades.
* **Decision:** Tiered backoff and jitter by job sensitivity:
  - `notification`: 3 attempts, exponential backoff (500ms base, 20% jitter) for fast recovery.
  - `email`: 5 attempts, exponential backoff (2,000ms base, 50% wide jitter) to absorb downstream 429 rate limits.
  - `image-processing`: 3 attempts, fixed backoff (10,000ms base, 30% jitter) for transient CDN timeouts.
* **Alternatives Considered:**
  - *Uniform Immediate Retry:* Exhausts retry budget instantly across consecutive milliseconds of external API downtime.
  - *Fixed Interval Without Jitter (e.g. 5s across all jobs):* Induces Thundering Herd where hundreds of failed jobs retry in synchronized discrete waves.
  - *Pure Exponential Backoff Without Jitter:* Synchronized retries still clump in discrete waves ($2^n$); randomized fractional jitter mathematically desynchronizes arrival curves.
* **Trade-off:** Flattens retry spikes across time; retrying jobs temporarily occupy slots in Redis `delayed` set.

---

### ADR-004: Terminal vs. Transient Error Handling (`UnrecoverableError`)
* **Status:** Accepted | **Date:** 2026-10-23
* **Context:** Deterministic errors (malformed emails, broken image URLs) repeatedly retried, wasting API credits and compute.
* **Decision:** Guard worker handlers with input validation (`isValidEmail`, `isValidImage`). Throws `UnrecoverableError` / `PoisonPillError` on schema violations, aborting remaining attempts immediately and routing directly to the `failed` set.
* **Alternatives Considered:**
  - *Blind Catch-All Retries:* Deterministic bugs (invalid email, malformed URL) retry 3–5 times, wasting worker CPU and third-party API quotas before dying.
  - *Manual Queue Removal via Catch Block:* Error-prone boilerplate that bypasses BullMQ's native Lua state machine.
  - *Auxiliary Quarantine Queue Producer:* Redundant; BullMQ natively shunts unrecoverable jobs straight to the `failed` set with stack traces preserved.
* **Trade-off:** Eliminates wasted compute on poison pills; requires strict validation logic to avoid falsely failing recoverable jobs.

---

### ADR-005: Redis Connection & `maxRetriesPerRequest: null`
* **Status:** Accepted | **Date:** 2026-10-19
* **Context:** BullMQ blocking commands (`BRPOPLPUSH`) trigger `ioredis` default retry limit exceptions (`maxRetriesPerRequest reached`), crashing workers during idle queue periods.
* **Decision:** Mandated `maxRetriesPerRequest: null` across all shared `ioredis` instances.
* **Alternatives Considered:**
  - *High Finite Retries (`maxRetriesPerRequest: 100`):* Idle periods on empty queues or managed failover delays (>30s) still hit the ceiling, crashing workers.
  - *Separate Connections for Blocking vs Normal Commands:* Creates configuration drift; BullMQ's contract mandates `null` across all shared connections.
  - *Global Process Error Swallowing (`uncaughtException`):* Anti-pattern that conceals connection leaks and leaves workers in zombie states.
* **Trade-off:** Prevents idle crashes and enables automatic recovery across Redis restarts; commands buffer in memory if Redis disconnects permanently.

---

### ADR-006: Deterministic Two-Phase Graceful Shutdown
* **Status:** Accepted | **Date:** 2026-10-20
* **Context:** Worker termination mid-job leaves Redis locks orphaned (causing 30s stalled delays) and fails to catch `SIGINT` on Windows consoles.
* **Decision:** Implemented two-phase shutdown in `worker-runner.ts`:
  1. `worker.close()` stops accepting new jobs and drains active jobs.
  2. `redisConnection.quit()` cleanly closes client sockets.
  3. 10-second unreferenced watchdog timer (`setTimeout(..., 10000).unref()`) prevents hung processes.
* **Alternatives Considered:**
  - *Immediate Exit (`process.exit(0)` on signal):* Aborts in-flight jobs midway, leaving locks stranded in Redis until stalled-job timeouts expire (30s delay).
  - *Indefinite Wait on `worker.close()`:* Hanging external HTTP calls or infinite loops would block deployments indefinitely.
  - *Relying Exclusively on Docker/K8s SIGKILL:* Abrupt SIGKILL leaves Redis state unclean and causes unnecessary alert noise.
* **Trade-off:** Zero aborted jobs during deployments; requires container orchestrators to allow $\ge 15\text{s}$ termination grace periods.

---

### ADR-007: Job Retention & Memory Pruning
* **Status:** Superseded & Refined by ADR-010 | **Date:** 2026-10-19
* **Context:** Redis memory grows monotonically if completed/failed jobs are retained indefinitely.
* **Decision:** Capped completed jobs to `removeOnComplete: 1000`. Failed jobs retained for dead-letter analysis (initially `removeOnFail: false`, capped to `1000` in ADR-010).
* **Alternatives Considered:**
  - *Immediate Total Deletion (`removeOnComplete: true`, `removeOnFail: true`):* Prevents operational triage—engineers cannot inspect payloads or stack traces to debug issues.
  - *Uncapped Permanent Retention:* Consumes unbounded Redis RAM, leading to OOM kills under sustained volume.
  - *Time-To-Live Eviction (`age: 3600`):* Memory footprint scales unpredictably with ingestion spikes; count-based limits (`1000`) guarantee a fixed upper memory bound.
* **Trade-off:** Limits Redis memory usage to bounded ring-buffers; historical metrics beyond 1,000 jobs must be shipped to external telemetry.

---

### ADR-008: In-Worker Lifecycle Telemetry
* **Status:** Accepted | **Date:** 2026-10-23
* **Context:** Standard BullMQ events only fire after an attempt finishes, leaving zero real-time visibility into in-flight attempt indices.
* **Decision:** Worker prints structured stdout diagnostic markers on execution start:
  `[START] Processing {name} (Priority: {p}, ID: {id}, Attempt: {attemptsMade + 1}/{opts.attempts})`.
* **Alternatives Considered:**
  - *Relying Only on Global Worker Events (`worker.on('active')`):* Emits globally without contextual correlation with in-flight handler branch execution.
  - *Heavy APM Tracer Libraries (OpenTelemetry/Datadog):* Deferred to larger scale; adds heavy agent overhead during local dev and testing.
  - *Polling Redis Metadata Externally:* Incurs extra Redis roundtrips and lacks millisecond synchronization with real-time execution.
* **Trade-off:** Universal visibility across Docker/K8s logs with zero external APM dependencies; adds minor stdout volume.

---

### ADR-009: Empirical Jitter Verification Harness
* **Status:** Accepted | **Date:** 2026-10-24
* **Context:** Randomized jitter ($ \pm 50\% $) causes consecutive retry intervals to overlap, causing naive monotonic assertions (`delays[i] > delays[i-1]`) to fail flakily in CI/CD.
* **Decision:** Built `src/verify-retries.ts` with a parameterized `verifyWindows()` assertion model:
  $$\text{window}_i = \big[\, \text{nominal} \cdot (1 - \text{jitter}) - \text{buffer}, \;\; \text{nominal} + \text{buffer} \,\big]$$
  Validates that actual measured retry deltas strictly fall within mathematical bounds while absorbing socket roundtrips.
* **Alternatives Considered:**
  - *Strict Monotonic Inequality (`delays[i] > delays[i-1]`):* When jitter $\ge 50\%$, interval bounds collide, producing false-negative flaky CI failures under minor event-loop lag.
  - *Mocking Redis (`ioredis-mock`):* Doesn't execute BullMQ Lua scripts or Redis delayed ZSET polling, hiding real-world timing bugs.
  - *Manual Stop-Watch Log Auditing:* Error-prone, non-repeatable, and untestable in automated CI/CD pipelines.
* **Trade-off:** Deterministic, non-flaky retry verification; test suite takes ~35s waiting on genuine Redis delay timers.

---

### ADR-010: Native Dead-Letter Architecture & Redlock Replay
* **Status:** Accepted | **Date:** 2026-10-29
* **Context:** Uncapped failed jobs threatened Redis OOM. Automated replays lacked poison pill guards, risking infinite retry loops and concurrent race conditions.
* **Decision:**
  1. **Capped DLQ Retention:** Configured `removeOnFail: { count: 1000 }` on native failed sets.
  2. **Memory Policy:** Enforced `maxmemory-policy noeviction` via `ensureRedisNoEviction()`.
  3. **Bounded Replay Ceiling:** Replay service tracks `_replayCount`. Jobs exceeding 3 replay cycles are permanently escalated to poison pills.
  4. **Distributed Replay Lock:** `inspectAndReplayFailedJobs()` acquires Redlock on `lock:replay:<queueName>` (30s TTL) to prevent duplicate execution across admin instances.
  5. **Concurrency Ceiling:** Fixed at `concurrency: 1` to guarantee serial processing and avoid 429 cascades.
* **Alternatives Considered:**
  - *Secondary Auxiliary DLQ Queue (`my-queue-dlq`):* Duplicates jobs, creates new IDs, breaks provenance, and requires dual-queue maintenance.
  - *Uncapped Failed Job Retention:* Prolonged outages fill Redis RAM until OOM crash.
  - *Automated Infinite Replay:* Persistent failures loop forever, creating infinite log spam and third-party API costs.
  - *Unsynchronized Replay Execution:* Multiple admin pods pull the same failed jobs concurrently, triggering duplicate executions.
* **Trade-off:** Memory-safe, poison-pill-resilient DLQ; jobs exceeding 1,000 failures are pruned from Redis.

---

### ADR-011: Edge-Decoupled REST API Ingress & SHA-256 Idempotency
* **Status:** Accepted | **Date:** 2026-10-31
* **Context:** Direct Redis access tightly couples external clients to internal broker credentials and drivers, lacks ingress validation, and allows network retry storms to create duplicate jobs.
* **Decision:** Built Express REST API (`src/api-server.ts`) with Redis-backed `IdempotencyManager` (`src/idempotency.ts`):
  1. **Strict Ingress Validation & `202 Accepted`:** Validates schemas synchronously (400 on error), returns `202 Accepted` with `Location` header.
  2. **SHA-256 Cryptographic Idempotency:**
     - Computes canonical `SHA-256(Body)`.
     - Claims Redis lease via `SET idempotency:<key> {"status":"PROCESSING"} NX EX 60`.
     - Rejects altered payloads reusing the same key with `422 Unprocessable Entity`.
     - Rejects parallel requests with `409 Conflict`.
     - Caches successful 202 response for 24h (`86400s`).
     - Releases lock (`DEL`) on enqueue failure for immediate client retry.
  3. **Unified Observability:** `GET /api/v1/jobs/:id` (and `/progress`) returns state, progress, timestamps, and error traces.
  4. **DLQ Control Plane:** `GET /api/v1/dlq/jobs` (paginated audit with poison pill classification), `POST /api/v1/dlq/jobs/:id/retry` (single job replay), and `POST /api/v1/dlq/retry-batch` (Redlock-synchronized batch replay).
  5. **Health Probing:** `GET /health` pings Redis, returning 503 if broker is degraded.
* **Alternatives Considered:**
  - *Direct Broker Access for Microservices:* Exposes Redis port/credentials outside VPC, couples polyglot clients to Node.js/BullMQ drivers.
  - *Synchronous Blocking Execution over HTTP:* Defeats async decoupling, exhausts API connection pools, and triggers 504 Gateway Timeouts on long jobs.
  - *In-Memory Idempotency Cache:* Fails across horizontally scaled multi-pod APIs and is lost on process restarts.
  - *Raw Unhashed Idempotency Keys:* Buggy clients reusing keys across mutated bodies would receive cached results of unrelated jobs (payload poisoning).
* **Trade-off:** Complete network perimeter isolation and zero duplicate enqueues; adds ~2–5ms HTTP hop latency.

---

### ADR-012: In-Memory Throttled Progress Telemetry & On-Demand Polling
* **Status:** Accepted | **Date:** 2026-10-31
* **Context:** Divisible jobs (`image-processing`) require progress tracking. Unthrottled calls to `job.updateProgress()` trigger atomic Lua scripts and Pub/Sub broadcasts on every sub-chunk, saturating Redis's single thread and starving BullMQ's heartbeat lock extensions (`extendLock`), triggering false-positive stalled job recoveries.
* **Decision:** Implemented client-polled progress backed by an in-worker dual-gate rate limiter (`src/throttled-progress.ts`):
  1. **Dual-Gate In-Memory Rate Limiter:** Enforces `timeElapsed >= 1000ms` and `delta >= 5%`. Drops high-frequency intermediate ticks in worker RAM with zero Redis overhead.
  2. **Guaranteed Terminal Flush:** Final `tracker.flush(100)` bypasses throttling, guaranteeing Redis reflects strictly 100% completion upon task finish.
  3. **Selective Instrumentation:** Sub-second atomic jobs (`notification`, `email`) omit progress writes entirely; state transitions (`waiting` $\to$ `active` $\to$ `completed`) suffice.
  4. **On-Demand Stateless Polling:** `GET /jobs/:id/progress` reads `job.getState()` and `job.progress` on demand, returning `404 Not Found` if pruned by `removeOnComplete`.
* **Alternatives Considered:**
  - *In-Memory Event Emitters / Pub/Sub Push:* Fails across OS process boundaries, horizontal multi-pod replicas, and transient HTTP request lifecycles.
  - *Unthrottled `job.updateProgress()`:* Induces Head-of-Line Redis blocking, delayed lock heartbeats, and duplicate execution loops.
  - *Database Write on Every Tick:* Saturates relational databases with row-lock contention and excessive write IOPS.
* **Trade-off:** Quantizes progress telemetry to 1-second intervals in exchange for 75–90% Redis write reduction and zero lock starvation.

---

### ADR-013: Multi-Role Observability, Bull Board UI Partitioning & Fail-Safe RBAC
* **Status:** Accepted | **Date:** 2026-11-02
* **Context:** Operators require interactive controls (retrying DLQ, pausing, and deleting jobs), whereas general stakeholders and support engineers require read-only visibility into queue depth, active throughput, and error rates without mutating permissions. Exposing unauthenticated or single-tenant dashboards poses catastrophic risk (unauthorized queue flushing or poison-pill re-injection). Furthermore, Bull Board's client-side `readOnlyMode` only hides UI buttons without blocking raw HTTP mutating requests.
* **Decision:**
  1. **Dual Mount-Point Partitioning:** Mounted separate Bull Board instances on `/admin/queues` (`readOnlyMode: false`) and `/user/queues` (`readOnlyMode: true`).
  2. **Strict Fail-Safe RBAC:** In `src/monitoring-auth.ts`, `checkUserRole()` enforces default-deny: if an account has missing, null, or empty roles, it is strictly denied both admin and user access (`403 Forbidden`). Only accounts with explicit `'admin'` in their roles array can access the admin console.
  3. **Defense-in-Depth HTTP Method Guard:** `enforceReadOnlyMethods()` sits ahead of `/user/queues`, rejecting any non-idempotent HTTP method (`POST`, `PUT`, `DELETE`, `PATCH`) with `403 Forbidden` to neutralize DevTools/curl bypass attempts.
  4. **Timing-Safe Authentication:** Credentials evaluated via `crypto.timingSafeEqual` over HTTP Basic Auth headers.
  5. **Programmatic Telemetry Endpoint:** Implemented `GET /api/v1/metrics` querying $O(1)$ counts (`queue.getJobCounts()`) with broker readiness fallback (returning 503 when offline).
* **Alternatives Considered:**
  - *Single Shared Dashboard with UI-Only Buttons Disabled:* Malicious or curious users can inspect network payloads and issue direct `PUT /admin/queues/api/queues/.../retry` requests.
  - *Separate Standalone Monitoring Service:* Adds operational complexity, separate port management, and cross-VPC networking overhead.
  - *Permissive Missing Role Fallback:* Defaulting missing roles to viewer or admin creates privilege escalation vulnerabilities.
* **Trade-off:** Requires managing two Bull Board instances in memory; provides mathematical role guarantees and complete mitigation against unauthorized queue mutations.

---

### ADR-014: Decoupled CacheCut Click-Sync Consumer Engine & Scheduler Pruning
* **Status:** Accepted | **Date:** 2026-11-03
* **Context:** CacheCut is a high-throughput URL shortener servicing thousands of requests per second. Click counting was historically executed via an in-process `setInterval` running database updates in the Express HTTP event loop. This caused multi-pod race conditions under horizontal scaling, blocked HTTP redirects during PostgreSQL latency spikes, and risked permanent click loss due to fire-and-forget `.catch()` recovery during database outages.
* **Decision:**
  1. **Flagship Consumer Task:** Integrated CacheCut click persistence as Qrew's first production workload (`JobTypes.AppJob -> 'click-sync'`).
  2. **Decoupled Architecture:** CacheCut retains only its sub-millisecond local memory-to-Redis flush (`clicks:<short_code>` and `dirty:clicks`). All relational database persistence is fully offloaded to Qrew.
  3. **BullMQ v6 Native Scheduler Engine:** Configured recurring execution via `queue.upsertJobScheduler()` running on a 5,000ms cadence at `JobPriority.HIGH`. Standardized on BullMQ v6's descriptor format where schedulers are keyed via `.key` (addressing legacy `.id` lookup mismatches).
  4. **Atomic Handoff & Failure Recovery:** Worker pops from `dirty:clicks` and performs atomic `GETDEL clicks:<code>`. If PostgreSQL operations fail, counts are immediately re-accumulated into Redis (`INCRBY` + `SADD`) before throwing, allowing BullMQ's exponential backoff, jitter, and DLQ to govern retries safely.
  5. **Bounded Memory & Log Pruning:** Applied `removeOnComplete: 100` and `removeOnFail: 500` to prevent empty 5-second recurring ticks from saturating Redis memory while preserving failed job traces for investigation.
* **Alternatives Considered:**
  - *In-Process CacheCut `setInterval`:* Contends for PostgreSQL connection pools and steals event-loop cycles from the redirect path.
  - *HTTP API Push Ingress on Every Flush:* Introduces outbound network socket latency and failure modes into CacheCut.
* **Trade-off:** Introduces a bounded 5-second eventual consistency window for PostgreSQL analytics in exchange for total redirect isolation, serialized execution, and guaranteed zero click loss.

---

## 4. Failure Modes & Mitigation Matrix

| Failure Mode | Immediate Behavior | Built-In Mitigation | Production Scaling Mitigation |
| :--- | :--- | :--- | :--- |
| **Worker Crash Mid-Job** | Job left in `active` state. | BullMQ lock expires; stalled monitor returns job to queue. | Ensure worker tasks are idempotent at DB level. |
| **Click-Sync PostgreSQL Outage** | Worker throws during SQL UPDATE. | Counts restored to Redis (`INCRBY` + `SADD dirty:clicks`); BullMQ applies exponential backoff + jitter; DLQ catches exhausted attempts. | Read replica offloading or Kafka CDC pipeline. |
| **Downstream Rate Limit (429)** | Provider returns 429. | Exponential backoff + 50% randomized jitter prevents thundering herd. | In-worker rate limiter (`limiter: { max, duration }`). |
| **Poison Pill Job** | Malformed input. | Schema guard throws `PoisonPillError`, short-circuiting to `failed`. | Automated quarantine alerts to Slack/PagerDuty. |
| **Redis Outage / Partition** | Broker socket drops. | `maxRetriesPerRequest: null` queues commands; reconnects automatically. | Redis Sentinel or AWS ElastiCache Multi-AZ failover. |
| **Priority Starvation** | P1 flood blocks P3. | Inherent to strict single-queue priority. | Multi-Queue worker pools (dedicated P1, P2, P3 workers). |
| **Redis Memory Pressure** | Backlog grows. | `removeOnComplete: 1000`, `removeOnFail: 1000`, `noeviction` policy. | S3 external payload storage; Redis memory alerts. |
| **Infinite Replay Loop** | Persistent failure retried. | `maxReplays: 3` ceiling permanently flags job as poison pill. | Dead-letter forwarding to BigQuery/Elasticsearch. |
| **Concurrent Replay Race** | Multiple admins replay DLQ. | Redlock (`lock:replay:<queue>`) serializes replay execution. | OpenTelemetry distributed tracing span correlation. |
| **Network Retry Submission** | Duplicate HTTP POST. | Ingress Idempotency Manager caches response; returns duplicate 202. | 24-hour Redis idempotency lease TTL. |
| **Idempotency Key Mutation** | Same key, altered body. | Canonical SHA-256 fingerprint mismatch returns `422 Unprocessable Entity`. | Enforce unique UUIDv4 headers on client SDKs. |
| **Broker Down on Ingestion** | Enqueue fails. | Returns `503 Service Unavailable`, deletes lease for immediate retry. | Circuit breaker + Transactional Outbox in SQL. |
| **Progress Write Saturation** | High-frequency progress updates flood Redis. | In-memory dual-gate throttler (`1000ms`, `5%`) caps writes; terminal `flush()` ensures 100% completion. | Offload high-frequency chunk telemetry to Redis Streams or separate ephemeral cache cluster. |

---

## 5. Load Testing & Invariant Verification

Empirically validated via `src/priority-load-test.ts` (550 jobs enqueued in reverse order: 250 Medium, 200 High, 100 Critical):
- **Global Priority Invariant:** **PASSED**. 100 Critical jobs completed first, followed by 200 High, followed by 250 Medium.
- **Intra-Priority FIFO Invariant:** **PASSED**. Within each priority level, jobs executed in strict insertion order (`seq` monotonically increasing).
- **Throughput:** ~1,100 jobs/sec on a single worker core under saturation.
- **Throttled Progress Invariant (`src/verify-progress.ts`):** **PASSED**. Multi-chunk task executed 20 progress iterations; throttler eliminated ~75% of redundant writes to Redis while terminal `flush(100)` guaranteed exact completion state on API read.
- **CacheCut Click-Sync Consumer Invariant (`src/verify-cachecut-sync.ts`):** **PASSED**. Validated atomic `GETDEL` synchronization (35 clicks flushed across 2 URLs), zero-leak Redis cleanup, BullMQ v6 scheduler key resolution, and silent zero-overhead handling of empty cadences.

---

## 6. Scaling Roadmap (100x Scale)

1. **Dedicated Priority Queues:** Split `my-queue` into `queue:critical`, `queue:high`, and `queue:medium` with weighted worker allocation (e.g., 60% / 30% / 10%) to prevent starvation.
2. **KEDA Dynamic Autoscaling:** Scale Kubernetes worker pods dynamically from 2 to 50 based on Redis queue depth (`bull:my-queue:prioritized` cardinality).
3. **Out-of-Process CPU Sandboxing:** Move heavy image transformations to BullMQ Sandboxed child processes with isolated Libuv threadpools (`UV_THREADPOOL_SIZE >= 8`).
4. **Transactional Outbox & CDC:** Store jobs in an ACID database `outbox` table during Redis network outages, relaying asynchronously once broker reconnects.
5. **OpenTelemetry Propagation:** Inject W3C `traceparent` headers at HTTP ingress, tracing execution seamlessly across API $\to$ Redis $\to$ Worker $\to$ External APIs.
