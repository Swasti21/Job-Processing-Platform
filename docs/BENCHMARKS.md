# Load Test Benchmarks

## Overview
This benchmark evaluates throughput and scaling behavior for processing a batch of 5,000 synthetic documents through the distributed job processing pipeline (FastAPI + Kafka + PostgreSQL + Workers).

All tests were performed with `PROCESSING_DELAY_MS=0` and `FAILURE_RATE=0` to measure raw processing throughput.

---

## 1. Summary of Measured Results

| Configuration | Baseline Duration | Baseline Throughput | Post-Fix Duration | Post-Fix Throughput | Speedup |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **2 Workers** (concurrency=16) | 136.0 s | 37 docs/sec | 48.5 s | 103 docs/sec | **2.78x** |
| **4 Workers** (concurrency=16) | 98.9 s | 51 docs/sec | 50.8 s | 98 docs/sec | **1.92x** |

---

## 2. Detailed Run Metrics

### Baseline Runs (Before Fix)

- **2 Workers**:
  - **Batch ID**: `4433f7e3-9fba-415d-af33-8d4abdeb3d57`
  - **Submission Time**: 3.5 s
  - **Processing Time**: 136.0 s
  - **Throughput**: 37 docs/sec
  - **Status Counts**: `{'COMPLETED': 4895, 'FAILED': 105}`
  - **Field Accuracy**: 0.9812 (Document Exact Match: 0.8897)

- **4 Workers**:
  - **Batch ID**: `35a871e6-7d9e-4cf3-a930-b8169298c5da`
  - **Submission Time**: 5.1 s
  - **Processing Time**: 98.9 s
  - **Throughput**: 51 docs/sec
  - **Status Counts**: `{'COMPLETED': 4906, 'FAILED': 94}`
  - **Field Accuracy**: 0.9813 (Document Exact Match: 0.8901)

### Post-Fix Runs (After Atomic Counter & Session Consolidation)

- **2 Workers**:
  - **Batch ID**: `ccb6457d-c86d-4427-84ef-5535042b5914`
  - **Submission Time**: 1.1 s
  - **Processing Time**: 48.5 s
  - **Throughput**: 103 docs/sec
  - **Status Counts**: `{'COMPLETED': 4912, 'FAILED': 88}`
  - **Field Accuracy**: 0.9815 (Document Exact Match: 0.8917)

- **4 Workers**:
  - **Batch ID**: `db3f0d36-d747-4d65-962a-2393fb9a4d32`
  - **Submission Time**: 0.8 s
  - **Processing Time**: 50.8 s
  - **Throughput**: 98 docs/sec
  - **Status Counts**: `{'COMPLETED': 4905, 'FAILED': 95}`
  - **Field Accuracy**: 0.9805 (Document Exact Match: 0.8862)

---

## 3. Bottleneck Analysis & Evidence

### Investigation of Suspects

1. **Suspect (a): Retry messages sleep inside the gather loop in `app/worker.py` and stall the batch**
   - **Verdict**: **Rejected (Not the active bottleneck)**.
   - **Evidence**: During the load tests, `FAILURE_RATE` was set to 0. The 2% failed documents were generated with corrupt markers, raising non-retryable `PermanentError` exceptions. These went straight to `documents.dlq`, bypassing `documents.retry`. Zero retry messages were queued, yet throughput remained severely throttled at 37–51 docs/sec.

2. **Suspect (b): Every job updates the same `batches` row in `app/processing.py` (`_count_and_notify`)**
   - **Verdict**: **Confirmed (Primary Bottleneck)**.
   - **Evidence**:
     - `docker stats` during baseline runs showed `job-platform-postgres-1` under sustained heavy CPU load (reaching up to 331% CPU with 74 active process connections), while workers spent most of their time waiting on Postgres locks.
     - Isolated PostgreSQL micro-benchmarks:
       - 960 concurrent updates on **distinct rows** across 32 connections: **608.8 ops/sec** (1.577 s).
       - 960 concurrent updates on the **exact same row** across 32 connections: **78.0 ops/sec** (12.302 s) — an **8x degradation** purely due to row-level exclusive lock contention.
       - The original `_count_and_notify` executed 3 sequential queries (`UPDATE batches` counter -> `UPDATE batches` status -> `SELECT batches`) while holding the transaction open and retaining the row lock.

---

## 4. Fix Applied

1. **Atomic Batch Progress Update (`app/processing.py`)**:
   - Replaced 3 separate SQL round-trips in `_count_and_notify` with a single atomic `UPDATE ... RETURNING` statement using SQLAlchemy `case()`.
   - The counter increment, conditional `DONE` transition, and `completed_at` assignment are evaluated in a single database operation, drastically shortening the exclusive row lock duration on `batches`.
2. **Unified Claim and Fetch Session (`app/processing.py`)**:
   - Combined `claim_job` and `Document.data` retrieval into a single database session in `process_message`.
   - Avoids opening and closing 5,000 separate database sessions per batch.
3. **Preserved Semantics**:
   - Maintained at-least-once delivery, idempotent transitions, dead-letter queue routing, retries, live WebSocket progress via `pg_notify`, and `BUS=local` mode.
   - Preserved all table schemas, message schemas, and API contracts.
