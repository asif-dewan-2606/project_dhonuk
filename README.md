# Dhonuk Streaming Platform

A configuration-driven Kafka ingestion framework. New pipelines (new topic →
new destination) are added by editing YAML, not by writing new Python.

```
Oracle ──▶ Kafka Connect (JDBC) ──▶ Kafka ──▶ Python Consumer ──▶ ClickHouse
                                       ▲
Fake-data Producer ────────────────────┘   (testing only, independent of Oracle)
```

---

## Table of contents

1. [Project layout](#1-project-layout)
2. [The two moving parts: producer vs consumer](#2-the-two-moving-parts-producer-vs-consumer)
3. [Consumer framework — end-to-end flow](#3-consumer-framework--end-to-end-flow)
4. [How Kafka is used here](#4-how-kafka-is-used-here)
5. [Offset management — who commits, and when](#5-offset-management--who-commits-and-when)
6. [Failure behavior — what happens if something breaks](#6-failure-behavior--what-happens-if-something-breaks)
7. [Configuration reference](#7-configuration-reference)
8. [How to add a new pipeline](#8-how-to-add-a-new-pipeline)
9. [How to add a new sink type](#9-how-to-add-a-new-sink-type)
10. [Producer (fake data generator)](#10-producer-fake-data-generator)
11. [Docker / Compose](#11-docker--compose)
12. [Logging](#12-logging)
13. [Design decisions & deliberate non-choices](#13-design-decisions--deliberate-non-choices)

---

## 1. Project layout

```
.
├── docker-compose.yml
├── images/
│   ├── consumer/Dockerfile
│   └── producer/Dockerfile
│
├── producer/                        # independent fake-data generator
│   ├── requirements.txt
│   └── src/
│       ├── main.py                  # entrypoint: generate → publish loop
│       ├── config.py                # env-driven settings
│       ├── generator.py             # TransactionGenerator (random fake txns)
│       ├── models.py                # Transaction dataclass
│       └── publishers/
│           ├── base.py              # Publisher ABC
│           ├── console.py           # prints to stdout (local testing)
│           ├── kafka.py             # publishes to Kafka
│           └── factory.py           # picks publisher by config.PUBLISHER
│
└── consumer/                        # the actual ingestion framework
    ├── requirements.txt
    └── src/
        ├── main.py                  # entrypoint
        ├── config/
        │   ├── config.py            # YAML loader + ${VAR} substitution
        │   ├── application.yaml     # global settings (Kafka, sink connections)
        │   └── pipelines.yaml       # ALL pipelines are defined here
        ├── registry.py              # builds runners from config
        ├── manager.py               # runs the poll/flush/commit loop per pipeline
        ├── pipeline.py              # buffering + batching + flush (one class, shared)
        ├── consumers/
        │   └── kafka_consumer.py    # thin wrapper: poll / commit / close
        └── sinks/
            ├── base.py              # Sink ABC: write() / close()
            ├── clickhouse.py        # implemented
            ├── postgres.py          # stub, NotImplementedError
            ├── ozone.py             # stub, NotImplementedError
            └── factory.py           # maps sink "type" string → class
```

**Rule of thumb:** if you're changing *what* gets ingested or *where* it
goes, you're editing `pipelines.yaml` (and maybe writing one new `Sink`
class). You should basically never need to touch `pipeline.py`,
`manager.py`, or `registry.py` — those are the framework, not the config.

---

## 2. The two moving parts: producer vs consumer

These are **two unrelated processes** that happen to share a Kafka topic
during local testing:

- **Producer** (`producer/`) — generates fake transactions and publishes
  them to Kafka, purely so you have something to test the consumer against
  without a real Oracle + Kafka Connect setup. In production, **Kafka
  Connect (JDBC Source) replaces this entirely** — the producer is never
  involved in the real Oracle pipeline. You can turn it off and the
  consumer framework doesn't care; it just reads from whatever is producing
  to its configured topic.

- **Consumer** (`consumer/`) — the actual framework. Reads from one or
  more Kafka topics, batches records, writes them to a destination
  (ClickHouse today; Postgres/Ozone once implemented). This is the part
  that's meant to be generic and long-lived.

They share the topic name `sales_transactions` (via a single `KAFKA_TOPIC`
env var — see [§11](#11-docker--compose)) only so local `docker compose up`
gives you an end-to-end demo. In production the consumer's `pipelines.yaml`
would point at whatever topic Kafka Connect's JDBC Source writes to (e.g.
`oracle.IAS_DFS_TXN_LOG`), and the producer container simply wouldn't run.

---

## 3. Consumer framework — end-to-end flow

```
docker compose up consumer
        │
        ▼
main.py
        │
        │ 1. config.load_config(config_dir)
        │      reads application.yaml + pipelines.yaml
        │      substitutes ${VAR} placeholders from the container's env
        ▼
        │ 2. registry.build_pipelines(application, pipelines_config)
        │      for every entry in pipelines.yaml where enabled: true:
        │        a. look up sink type ("clickhouse") in sinks/factory.py
        │        b. merge application.yaml's connection block
        │           (host/port/user/password) with the pipeline's own
        │           sink block (table/columns) into one kwargs dict
        │        c. Sink(**merged_kwargs)                 → e.g. ClickHouseSink
        │        d. Pipeline(sink=..., batch_size=..., flush_interval_ms=...,
        │                     max_buffer_size=..., datetime_fields=...)
        │        e. KafkaConsumerClient(bootstrap_servers, group_id, topic)
        │      returns a list of PipelineRunner(name, consumer, pipeline)
        ▼
        │ 3. manager.run_all(runners)
        │      spawns one Python thread per runner
        ▼
   ┌─────────────────────────── per-thread loop (manager.run_pipeline) ──────────────────────────┐
   │                                                                                                │
   │   while True:                                                                                  │
   │       msg = consumer.poll()              # blocks up to 1s, returns None on timeout            │
   │                                                                                                 │
   │       if msg is None:                                                                          │
   │           if pipeline.is_ready(): flush_and_commit()   # time-based flush even with no traffic │
   │           continue                                                                              │
   │                                                                                                 │
   │       if msg.error():                                                                           │
   │           log it, continue (message is NOT added to buffer, NOT committed)                     │
   │                                                                                                 │
   │       pipeline.add(msg.value())          # JSON-decode, convert datetime_fields, buffer it      │
   │                                                                                                 │
   │       if pipeline.is_ready():                                                                   │
   │           flush_and_commit()                                                                    │
   │               → pipeline.flush()          # sink.write(batch); only clears buffer on success    │
   │               → consumer.commit()          # ONLY reached if flush() didn't raise                │
   │                                                                                                 │
   └────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Each pipeline is fully independent: its own `KafkaConsumerClient`, its own
`Pipeline` (buffer + batch state), its own thread. Pipeline A being slow or
stuck does not block pipeline B.

---

## 4. How Kafka is used here

- **Client library:** `confluent-kafka` (the librdkafka-based Python
  client), used directly — no extra abstraction on top of it beyond
  `consumers/kafka_consumer.py`, which only wraps `poll` / `commit` /
  `close` / subscribe.
- **Consumer group:** set per pipeline via `application.yaml`'s
  `kafka.consumer_group` (defaults to `<pipeline_name>-group` if omitted).
  Two pipelines reading the *same* topic with *different* group IDs each
  get their own independent copy of every message — that's standard Kafka
  consumer-group semantics, not anything custom.
- **Offset reset policy:** `auto.offset.reset: earliest` — a brand-new
  consumer group with no committed offset starts from the beginning of the
  topic (not "latest"). This matters if you spin up a new pipeline against
  an existing topic: it will backfill everything currently on the topic
  before catching up to live traffic.
- **Polling:** `consumer.poll(timeout=1.0)` — blocks up to 1 second waiting
  for a message, then returns `None` if nothing arrived. The 1-second
  timeout is also what lets a low-traffic pipeline still hit its
  `flush_interval_ms` on the "no message" branch of the loop, rather than
  waiting forever for the batch to fill by size alone.
- **Producing (in the consumer framework):** never happens. The consumer
  framework only reads from Kafka; it never writes back to it. (The
  *producer* project separately writes to Kafka for test data — see
  [§10](#10-producer-fake-data-generator).)

---

## 5. Offset management — who commits, and when

**Short answer: offsets are managed explicitly in our code, not by Kafka's
auto-commit.**

```python
"enable.auto.commit": False
```

This is set in `consumers/kafka_consumer.py` and is the single most
important line in the whole framework for data-safety. Here's why:

- With **auto-commit** (Kafka's default if you don't disable it), the
  client periodically commits the offset of whatever it has *polled*,
  regardless of whether your code actually finished processing it. If your
  process crashes after polling a message but before writing it to
  ClickHouse, Kafka may have already marked that offset as "done" — the
  message is silently lost forever.
- With **manual commit** (what this project does), we call
  `consumer.commit()` ourselves, and only in one place:
  `manager._flush_and_commit()`, immediately after
  `pipeline.flush()` returns *without raising*.

The actual commit sequence, per batch:

```python
def _flush_and_commit(runner):
    runner.pipeline.flush()     # 1. write the batch to the sink
    runner.consumer.commit()    # 2. only runs if step 1 didn't raise
```

`Pipeline.flush()` (in `pipeline.py`) is written so the in-memory buffer is
only cleared *after* `sink.write()` succeeds:

```python
def flush(self) -> None:
    if not self._buffer:
        return
    batch = self._buffer
    self.sink.write(batch)          # <- raises here if the write fails
    self._buffer = []               # <- only reached on success
    self._buffer_start = None
```

So the guarantee is:

| Scenario | What happens |
|---|---|
| Write succeeds | buffer clears, offset commits — message fully processed |
| Write fails (all retries exhausted) | exception propagates, buffer is **not** cleared, offset is **not** committed, exception crashes the pipeline thread → process exits → container restarts → consumer re-subscribes at the **last committed offset** → the same messages are re-polled and re-attempted |
| Process crashes mid-batch (before flush is even called) | nothing was committed for those buffered records either — same replay-on-restart behavior |

This gives **at-least-once delivery**: a message is never marked "done"
until it's actually landed in the sink. The trade-off (inherent to
at-least-once, not specific to this code) is that a crash *between* a
successful write and the commit call could, in principle, cause the same
batch to be reprocessed once. That's a standard, accepted trade-off for
this pattern — ClickHouse inserts here are not deduplicated/idempotent, so
in a rare crash-at-that-exact-instant scenario you could get a duplicate
batch. If exactly-once matters for a given sink, that sink would need
idempotent writes (e.g. ClickHouse `ReplacingMergeTree` with a dedup key,
or a Postgres `ON CONFLICT DO NOTHING`) — not something this framework
does generically today.

---

## 6. Failure behavior — what happens if something breaks

The rule throughout the codebase: **never swallow an exception silently.**
Something is always either logged, or logged *and* re-raised. Here's the
behavior for every failure point:

### a. Sink write fails (e.g. ClickHouse is down)

`sinks/clickhouse.py`'s `write()` retries with backoff:

```python
attempt = 0
while True:
    attempt += 1
    try:
        self.client.insert(...)
        return
    except Exception:
        logger.exception("ClickHouse insert failed (attempt %d/%d, %d rows)", ...)
        if attempt >= self.max_retries:
            raise
        time.sleep(self.retry_backoff_seconds * attempt)
```

- Every failed attempt is logged with a full traceback (`logger.exception`).
- Backoff is linear: `retry_backoff_seconds * attempt` (default
  `2s, 4s, 6s...`).
- After `max_retries` (default 3), it gives up and re-raises.

### b. That exception reaches `manager.py`

`pipeline.flush()` propagates it, `_flush_and_commit()` propagates it,
`run_pipeline()` catches it **only to log it**, then re-raises:

```python
except Exception:
    logger.exception("[%s] pipeline crashed", name)
    raise
finally:
    consumer.close()
    pipeline.sink.close()
```

The thread dies. That pipeline's Kafka consumer and sink connection are
closed cleanly on the way out (`finally`), but no more messages are
processed by that pipeline.

### c. `manager.run_all()` notices the dead thread

```python
while True:
    for t in threads:
        t.join(timeout=1)
        if not t.is_alive():
            logger.critical("Pipeline thread '%s' died - exiting process", t.name)
            os._exit(1)
```

**The entire process exits — deliberately** — even though other
pipelines' threads might still be healthy. This is a conscious design
choice: a half-alive process (2 of 3 pipelines running, 1 silently dead)
is worse than a clean restart, because a silently-dead pipeline is very
easy to miss in monitoring. `restart: unless-stopped` in
`docker-compose.yml` means Docker immediately restarts the container, all
pipelines re-initialize from their last committed offsets, and you get a
clean recovery instead of a slow data gap nobody notices.

### d. A single malformed Kafka message (bad JSON, missing field)

Currently: `pipeline.add()` calls `json.loads()` and dict lookups with no
try/except around them, so **a malformed message will raise, crash that
pipeline's thread the same way a sink failure would, and (per §c) bring
the whole process down.** This is the one place you may want to add a
narrower catch (e.g. log-and-skip malformed messages to a dead-letter
topic) if bad data becomes a real concern — deliberately left as "crash
loudly" for now rather than silently dropping bad records, in keeping with
the "never swallow exceptions" rule. If you add that, do it explicitly and
log every skipped message.

### e. Kafka broker itself is unreachable at startup

`confluent-kafka`'s underlying client handles reconnection/backoff
internally at the librdkafka level for the *consumer*. (Note: the
*producer's* `KafkaPublisher._wait_for_kafka()` has an explicit retry loop
for its own startup ordering against `docker-compose` — the consumer
doesn't currently have an equivalent explicit wait; `depends_on: kafka:
condition: service_started` in `docker-compose.yml` only waits for the
container to *start*, not for Kafka to be ready to accept connections. In
practice `poll()` calls simply return `None`/errors until Kafka is
reachable, so it self-recovers, but there's no explicit "waiting for
Kafka..." log line like the producer has.)

---

## 7. Configuration reference

### `application.yaml` — global, environment-wide settings

```yaml
kafka:
  bootstrap_servers: kafka:9092
  consumer_group: sales-consumer-group

clickhouse:
  host: clickhouse
  port: 8123
  database: analytics
  user: dhonuk
  password: ${CLICKHOUSE_PASSWORD}

postgres:
  host: postgres
  port: 5432
  database: analytics
  user: ${POSTGRES_USER}
  password: ${POSTGRES_PASSWORD}

ozone:
  endpoint: ${OZONE_ENDPOINT}
  bucket: ${OZONE_BUCKET}

logging:
  level: INFO
```

- Each top-level key other than `kafka`/`logging` (e.g. `clickhouse`,
  `postgres`, `ozone`) is a **connection block** for a sink type. The key
  name must match the sink `type` string used in `pipelines.yaml`.
- `${VAR}` anywhere in either YAML file is substituted with the
  container's environment variable of that name at load time (see
  `config/config.py::_substitute_env`). If the env var isn't set, it
  becomes an empty string — no error, so double-check secrets are actually
  set.
- `logging.level` sets the root log level: `DEBUG`, `INFO`, `WARNING`, or
  `ERROR`.

### `pipelines.yaml` — every pipeline, one list

```yaml
pipelines:
  - name: oracle_txn_to_clickhouse   # unique, used in logs & default group id
    enabled: true                    # false = registry skips it entirely
    topic: ${KAFKA_TOPIC}            # Kafka topic to consume
    batch:
      size: 500                     # flush once buffer reaches this many records
      flush_interval_ms: 100        # ...or once this many ms have elapsed, whichever first
      max_buffer_size: 5000         # hard ceiling - forces flush, logs a warning if hit
    datetime_fields: [approval_datetime, created, updated]
                                     # JSON string fields to parse into datetime objects
                                     # before handing the record to the sink
    sink:
      type: clickhouse               # must match a key in sinks/factory.py
      table: ias_dfs_txn_log
      columns: [id, sqn, ...]        # exact column order for the insert
```

- `batch.size` / `flush_interval_ms` / `max_buffer_size` are **per
  pipeline** — two pipelines can have completely different batching
  behavior.
- `sink` keys other than `type` are merged with `application.yaml`'s
  connection block of the same name and passed as constructor kwargs to
  the sink class — e.g. for `clickhouse`, `application.yaml` supplies
  `host/port/database/user/password`, `pipelines.yaml` supplies
  `table/columns`, and `ClickHouseSink(**merged)` gets all of them.
- A `type: spark` entry is recognized but **skipped by the registry** —
  Spark Structured Streaming pipelines don't run inside this process (see
  [§13](#13-design-decisions--deliberate-non-choices)).

---

## 8. How to add a new pipeline

**If the destination type already has a working `Sink`** (currently only
`clickhouse`), adding a pipeline is a pure YAML change:

```yaml
  - name: postgres_orders_to_clickhouse
    enabled: true
    topic: postgres.orders
    batch:
      size: 500
      flush_interval_ms: 200
      max_buffer_size: 5000
    datetime_fields: [created_at]
    sink:
      type: clickhouse
      table: orders_log
      columns: [id, customer_id, amount, created_at]
```

Add the block to `consumer/src/config/pipelines.yaml`, restart the
consumer container. Nothing else changes. The registry will build a
completely independent `KafkaConsumerClient` + `Pipeline` + `Sink` for it
and run it in its own thread alongside every other enabled pipeline.

**If the destination type doesn't have a `Sink` yet** (Postgres, Ozone),
see [§9](#9-how-to-add-a-new-sink-type) first, then add the YAML block the
same way.

**If it's a Spark pipeline** — this framework doesn't run it. Add it to
`pipelines.yaml` with `type: spark` purely as documentation of what
pipelines exist across the whole platform; build the actual job as its own
PySpark script (likely orchestrated by Airflow), separate from this repo's
consumer process.

---

## 9. How to add a new sink type

1. Open the stub in `consumer/src/sinks/` (`postgres.py` or `ozone.py`
   already exist with the right shape) and implement `write()`:

   ```python
   class PostgresSink(Sink):
       def __init__(self, host, port, database, user, password, table, columns):
           ...  # open connection

       def write(self, records: list[dict]) -> None:
           # convert records to rows, insert, RAISE on failure
           # (don't catch-and-log-only here - the framework needs the
           # exception to propagate so it doesn't commit the offset)

       def close(self) -> None:
           ...  # close connection
   ```

   The one hard rule: **`write()` must raise if the write didn't fully
   succeed.** That's what stops `manager.py` from committing offsets for
   data that didn't land.

2. Add its dependency to `consumer/requirements.txt` (e.g.
   `psycopg2-binary` for Postgres, `boto3` for Ozone's S3-compatible API).

3. It's already registered in `sinks/factory.py`:

   ```python
   _SINK_TYPES = {
       "clickhouse": ClickHouseSink,
       "postgres": PostgresSink,
       "ozone": OzoneSink,
   }
   ```

   If you're adding a genuinely new type (not Postgres/Ozone), add it here
   too.

4. Add its connection block to `application.yaml` and reference
   `type: <your_type>` in a `pipelines.yaml` entry.

`pipeline.py`, `manager.py`, `registry.py` — **none of these need to
change.** That's the entire point of the `Sink` abstraction.

---

## 10. Producer (fake data generator)

Purely a test-data tool — not part of the real Oracle pipeline.

```
main.py
   loop:
       transaction = generator.generate()      # random fake Transaction
       publisher.publish(transaction)           # console or kafka, via factory
       sleep(1 / EVENTS_PER_SECOND)
```

- `generator.py` — `TransactionGenerator.generate()` builds one random
  `Transaction` (dataclass in `models.py`) per call: random IDs, amounts,
  statuses, nullable fields, etc.
- `publishers/factory.py` picks the publisher based on `config.PUBLISHER`
  (`"console"` prints JSON to stdout for local debugging; `"kafka"`
  publishes to the configured topic).
- `publishers/kafka.py`:
  - Waits for Kafka to be reachable on startup (`_wait_for_kafka`, retries
    every 2s up to 30 times, logging each attempt).
  - Ensures the topic exists (`_ensure_topic_exists`) — creates it with 3
    partitions / replication factor 1 if missing. This is the **only**
    place in the whole codebase that creates a Kafka topic; the consumer
    never creates topics, only subscribes to existing ones.
  - `publish()` calls `producer.produce(...)` (async, fire-and-forget with
    a delivery callback that logs failures) then `producer.poll(0)` to
    trigger any pending callbacks — standard confluent-kafka pattern for a
    non-blocking producer loop.
  - `close()` calls `producer.flush()` to block until all in-flight
    messages are actually delivered before the process exits — this is
    what prevents losing the last few messages on shutdown.

Configuration is plain `os.getenv()` in `config.py` (no YAML) since the
producer is a single, simple, single-purpose script — it doesn't need the
multi-pipeline config system the consumer has.

---

## 11. Docker / Compose

- `producer` and `consumer` are separate services/images
  (`images/producer/Dockerfile`, `images/consumer/Dockerfile`), each just
  installing that project's `requirements.txt` — actual code is
  bind-mounted (`./producer/src:/app/src`, `./consumer/src:/app/src`) so
  you can edit and restart without rebuilding the image.
- **`KAFKA_TOPIC`** is a single compose-level variable
  (`${KAFKA_TOPIC:-sales_transactions}`), read by both the `producer` and
  `consumer` service blocks and ultimately sourced from your `.env` file.
  This is deliberate: the producer and consumer must agree on the topic
  name, so it's defined once, not typed independently in two places. If
  you rename it, change it once in `.env`.
- **Consumer secrets** (`CLICKHOUSE_PASSWORD`, `POSTGRES_USER`,
  `POSTGRES_PASSWORD`, `OZONE_ENDPOINT`, `OZONE_BUCKET`) are passed as
  plain env vars and picked up by `application.yaml`'s `${VAR}`
  placeholders at container startup. Non-secret connection details (host,
  port, database name) live directly in `application.yaml` — there's no
  reason to route those through the environment.
- `restart: unless-stopped` on the consumer means a crash (see §6) results
  in an automatic, clean restart.
- `depends_on: kafka: condition: service_started` only waits for the Kafka
  *container* to start, not for the broker to be ready to accept
  connections — see the note in §6.e.

---

## 12. Logging

Every `print()` from the original code has been replaced with `logging`
(the one deliberate exception: `ConsolePublisher.publish()`, whose entire
job *is* to print transaction JSON to stdout for local debugging — that's
output, not a log line).

- Configured once in `main.py` for both producer and consumer, format
  includes timestamp, level, thread name (useful since the consumer runs
  multiple pipelines as separate threads), logger name, and message.
- Levels used throughout:
  - `DEBUG` — per-batch flush confirmations (`pipeline.py`)
  - `INFO` — startup/shutdown of pipelines, topic subscription, pipeline
    build summary
  - `WARNING` — Kafka not ready yet (producer), disabled pipeline skipped,
    `max_buffer_size` reached
  - `ERROR` — a Kafka message carried an error, a delivery callback failed
  - `CRITICAL` — a pipeline thread died and the whole process is exiting
  - `logger.exception(...)` (ERROR level + full traceback) — sink write
    failures, uncaught exceptions in a pipeline thread
- Set the level via `application.yaml`'s `logging.level` (consumer) or
  `LOG_LEVEL` env var (producer).

---

## 13. Design decisions & deliberate non-choices

A few things that might look like gaps but were left out on purpose, in
keeping with "simple, not enterprise":

- **One `Pipeline` class, not one subclass per sink type.** The
  buffer/batch/flush logic is identical regardless of destination — only
  the `Sink` differs, and that's handled by composition
  (`Pipeline(sink=...)`), not inheritance. Adding a destination never
  means touching `pipeline.py`.
- **Spark pipelines aren't executed by this framework.** Structured
  Streaming has its own driver/executor runtime — it fundamentally isn't
  "poll Kafka in a loop," so forcing it into this consumer's
  `manager.py`/`Pipeline` shape would be the wrong abstraction. Spark jobs
  are documented in `pipelines.yaml` (commented, `type: spark`) but the
  registry skips them; the real job lives elsewhere.
- **Threading, not asyncio or multiprocessing.** Each Kafka consumer's
  `poll()` blocks only its own thread; plain `threading` is enough to run
  several pipelines concurrently in one process without the complexity of
  an async runtime.
- **No config schema/dataclass layer for YAML.** `application.yaml` and
  `pipelines.yaml` are loaded as plain dicts. With two small, human-edited
  files, a schema class is more code to keep in sync than value it adds.
  A typo shows up immediately as a `KeyError` at startup.
- **Any unrecoverable failure crashes the whole process**, not just the
  affected pipeline (§6.c) — a clean, visible restart beats a silently
  half-dead process.
- **At-least-once delivery, not exactly-once** (§5) — the standard,
  simplest trade-off for "commit after write." Exactly-once would require
  idempotent sink writes, which is a per-sink concern, not something to
  bake into the generic framework.
