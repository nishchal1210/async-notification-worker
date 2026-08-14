Your results are now strong enough to conclude the **one-worker versus three-worker scaling experiment**, with one reconciliation issue still to clean up.

## 1. Stage 1: API results from Locust

| Metric                 |   1 worker |  3 workers |
| ---------------------- | ---------: | ---------: |
| Total HTTP requests    |      2,785 |      2,539 |
| HTTP failures          |          0 |          0 |
| Average API throughput | 9.32 req/s | 8.49 req/s |
| p50 API latency        |       10 s |       11 s |
| p95 API latency        |       12 s |       13 s |
| p99 API latency        |       13 s |       14 s |
| Maximum API latency    |    14.75 s |    16.10 s |

The workload distribution remained close to your intended mixed load:

```text
Approximately 60% email
Approximately 30% push
Approximately 10% SMS
```

### Interpretation

Adding delivery workers did **not improve API ingestion**:

```text
1 worker: 9.32 requests/sec
3 workers: 8.49 requests/sec
```

This is expected because the worker count affects Stage 2:

```text
RabbitMQ → worker → provider
```

It does not directly speed up Stage 1:

```text
FastAPI → PostgreSQL → RabbitMQ publish → 202
```

The slight reduction from 9.32 to 8.49 RPS is ordinary run-to-run variation. It does not mean three workers harmed the API.

However, your API latency remains very high:

```text
p50 around 10–11 seconds
p95 around 12–13 seconds
```

That indicates Stage 1 still has a bottleneck, likely around the serialized RabbitMQ publisher, remote CloudAMQP latency, database commits, or the internal publisher queue.

---

# 2. Stage 2: worker delivery throughput

From your Prometheus exports:

| Metric                   |          1 worker |           3 workers |
| ------------------------ | ----------------: | ------------------: |
| Peak delivery throughput |          6.07/sec |            8.73/sec |
| Stable active throughput | roughly 5.8–6/sec | roughly 8.4–8.6/sec |

Three workers improved delivery throughput by approximately:

```text
8.73 / 6.07 ≈ 1.44×
```

So throughput improved by around **44%**, not 3×.

That is still a real improvement, but it shows that worker count is no longer the only constraint.

Possible shared bottlenecks include:

```text
PostgreSQL connection/commit capacity
RabbitMQ delivery rate
Remote network latency
Worker database queries
Metrics/logging overhead
Synchronous worker processing
```

A perfect three-times improvement should not be expected because all workers share the same PostgreSQL database, RabbitMQ broker, network, and machine.

---

# 3. Provider p95 latency

Your provider p95 values were almost identical across both tests.

| Channel |      1 worker |     3 workers |
| ------- | ------------: | ------------: |
| Email   | about 95.2 ms | about 95.2 ms |
| Push    | about 95.3 ms | about 95.0 ms |
| SMS     | about 94.6 ms | about 94.9 ms |

This is a very useful result.

It means:

> The fake provider itself remained stable and was not slowed down by adding workers.

The provider p95 is slightly above your configured 20–80 ms range because Prometheus histograms estimate the percentile from bucket boundaries. It does not mean the fake provider actually slept for exactly 95 ms.

The important conclusion is that provider performance stayed effectively unchanged.

---

# 4. Queue drain time

This is where the worker scaling benefit is extremely clear.

```text
One worker:    192.29 seconds
Three workers: 13.48 seconds
```

Improvement factor:

```text
192.29 / 13.48 ≈ 14.27×
```

Reduction:

```text
(192.29 - 13.48) / 192.29 × 100
≈ 93%
```

So three workers reduced queue drain time by approximately **93%**.

This is the strongest result in your experiment.

It means:

```text
One worker
↓
Large backlog remains after Locust stops
↓
Needs over 3 minutes to recover
```

versus:

```text
Three workers
↓
Backlog is processed concurrently
↓
Queue empties in about 13.5 seconds
```

The improvement being greater than 3× can happen because the two tests did not create exactly the same number of jobs and because timing, queue buildup, worker startup, and measurement boundaries differ.

Do not claim a precise 14× scalability improvement from this alone. State that the observed drain time improved by 14.3× in these particular local runs.

---

# 5. End-to-end p95 delivery latency

## One worker

Your end-to-end p95 rose sharply as the backlog accumulated:

```text
Initially: around 5–10 seconds
Later: hundreds of seconds
Maximum observed: around 531 seconds
```

This means many notifications were accepted by the API but waited several minutes before becoming `SENT`.

The high value is plausible because it includes:

```text
time waiting in RabbitMQ
+
worker processing
+
provider time
```

With a five-minute Prometheus rate window, the p95 can also remain elevated even after the queue begins draining.

## Three workers

The three-worker end-to-end p95 remained approximately:

```text
Typical: around 1.95–2.0 seconds
Maximum early value: around 3.4 seconds
```

This is a major improvement.

It shows the main latency in the one-worker case came from **queue waiting**, not the provider.

The provider still took roughly 95 ms p95, but one worker caused notifications to wait for minutes. Three workers largely prevented that backlog.

---

# 6. Reliability results

Both Locust runs show:

```text
HTTP failures: 0
Locust exceptions: 0
```

You also reported:

```text
No dead-lettered notifications
All selected notifications eventually became SENT
```

Therefore, within the populations you counted:

```text
Delivery success rate = 100%
Dead-letter rate = 0%
```

That confirms the retry/provider pipeline behaved correctly with:

```text
LOAD_TEST_PROVIDER_FAILURE_RATE=0
```

---

# 7. One remaining reconciliation issue

Your reported database totals do not exactly equal the Locust request totals.

## One worker

```text
Locust requests:       2,785
DB created and SENT:   2,883
Difference:               98
```

## Three workers

```text
Locust requests:       2,539
DB created and SENT:   2,640
Difference:              101
```

That means your database queries included extra rows from outside the exact Locust run, probably:

```text
manual test requests,
requests submitted just before the recorded start time,
previous test records,
or an overly broad timestamp filter.
```

This does **not** indicate lost messages because the database count is larger, not smaller. But you should not write that all Locust requests reconciled exactly until you isolate each run.

## Fix for the final benchmark

Add a unique test identifier to every payload:

```python
TEST_RUN_ID = "one-worker-20260805"

payload = {
    ...
    "subject": f"LOAD_TEST_{TEST_RUN_ID}",
}
```

Then query:

```sql
SELECT status, COUNT(*)
FROM notification.notifications
WHERE subject = 'LOAD_TEST_one-worker-20260805'
GROUP BY status;
```

A cleaner long-term approach is a separate column:

```sql
ALTER TABLE notification.notifications
ADD COLUMN test_run_id VARCHAR(100);
```

Then every run can be reconciled exactly.

---

# Final comparison

| Metric                 |                1 worker |   3 workers | Result                        |
| ---------------------- | ----------------------: | ----------: | ----------------------------- |
| API RPS                |                    9.32 |        8.49 | No meaningful API improvement |
| API p50                |                    10 s |        11 s | Nearly unchanged              |
| API p95                |                    12 s |        13 s | Nearly unchanged              |
| HTTP failures          |                       0 |           0 | Reliable Stage 1              |
| Peak worker throughput |                  6.07/s |      8.73/s | About 44% higher              |
| Provider p95           |             about 95 ms | about 95 ms | Unchanged                     |
| End-to-end p95         | rose to several minutes |   about 2 s | Dramatically better           |
| Queue drain time       |                192.29 s |     13.48 s | About 93% lower               |
| Dead-lettered          |                       0 |           0 | No delivery loss observed     |

# What this experiment proves

You can conclude:

> Increasing notification workers from one to three did not improve API ingestion latency or throughput because the API publishing path remained the Stage-1 bottleneck. However, it significantly improved asynchronous delivery performance. Peak worker throughput increased from approximately 6.07 to 8.73 deliveries per second, end-to-end p95 delivery latency dropped from several minutes under backlog to approximately two seconds, and queue drain time decreased from 192.29 seconds to 13.48 seconds. Provider p95 latency remained stable at approximately 95 ms, indicating that the improvement came from parallel worker processing rather than faster provider calls. Both runs completed without HTTP failures, dead-lettered notifications, or observed delivery failures.

## Is Milestone 16 complete?

Almost. You have completed:

```text
Load generation
Mixed workload testing
Fake-provider testing
One-worker benchmark
Three-worker benchmark
Locust metrics
Prometheus worker metrics
End-to-end latency measurement
Queue drain measurement
Reliability checks
Scaling comparison
```

The final two tasks are:

1. Isolate future runs with a `test_run_id` so DB and Locust counts reconcile exactly.
2. Write these results into `docs/load-test-report.md`.

After that, Milestone 16 is complete.
