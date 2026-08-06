# Project Dhonuk — Developer Guide

A practical reference for confidently modifying and extending Project Dhonuk.
This is not a line-by-line walkthrough — it explains control flow, responsibilities, and where to hook in new work.

---

## 1. Project Flow

Project Dhonuk is a streaming + batch data lakehouse pipeline. Transactional data is generated or polled, streamed through Kafka, landed as raw JSON, and progressively refined into governed Iceberg tables that are queryable through Trino.

```
Source (generator / Oracle / Postgres)
        ↓
   Kafka topic
        ↓
   ┌────┴─────┐
   ↓          ↓
Consumer   Kafka Connect (JDBC source connectors)
   ↓
   ├──→ ClickHouse (fast operational analytics table)
   └──→ Ozone (S3-compatible object store) — raw NDJSON, partitioned by date/hour
        ↓
   Spark job (raw_to_bronze)
        ↓
   Iceberg Bronze table
        ↓
   Polaris (Iceberg REST Catalog) — tracks table metadata/schema/location
        ↓
   Trino (iceberg.properties catalog) — SQL query engine over Iceberg
        ↓
   Query UI (CloudBeaver / any SQL client)
```

**Stage responsibilities:**

| Stage | Responsibility |
|---|---|
| **Source** | Emits transaction records — either synthetically (`producer`'s generator) or by polling real Oracle/Postgres tables (Kafka Connect JDBC source connectors). |
| **Kafka** | Durable, replayable transport between every stage. Two relevant topics exist today: `txn_events` (generator → ClickHouse) and `transaction_stream` (generator → Ozone raw, and reserved for downstream Spark streaming). |
| **Consumer** | A multi-pipeline batching engine. Each pipeline = one Kafka topic → one Sink (ClickHouse, Ozone, or Postgres). Buffers records and flushes on size/time thresholds, committing Kafka offsets only after a successful write. |
| **Raw JSON in Ozone** | The landing zone / "raw" layer. NDJSON files written by `OzoneSink`, partitioned as `prefix/year=/month=/day=/hour=/part-*.ndjson`. This is the durable, replayable source for all batch processing. |
| **Spark Job** | Reads raw data from Ozone and writes it into Iceberg tables (currently: `raw_to_bronze`). This is where schema is imposed on raw JSON. |
| **Iceberg Bronze** | The first governed table layer — lightly transformed, close to source shape. Stored as Iceberg tables backed by files in Ozone (S3-compatible), with metadata registered in Polaris. |
| **Polaris Catalog** | The Iceberg REST catalog. Owns table/namespace metadata, schema versions, and file locations. Both Spark and Trino talk to Polaris (not directly to each other) to resolve what a table currently looks like. |
| **Trino** | SQL query engine. Its `iceberg` catalog is configured to talk to Polaris, so any table Spark registers in Polaris becomes queryable in Trino without extra wiring. |
| **Query UI** | CloudBeaver (or any SQL client) connects to Trino to run ad hoc queries against Bronze/Silver/Gold tables. |

**Important reality check (read before assuming things are "live"):**
- The Bronze **Spark job currently only starts a Spark session and stops** — it does not yet read from Ozone or write to Iceberg. `readers/ozone.py` and `writers/iceberg.py` are empty stub files. Building these out is the natural first extension (see §5 and §6).
- **Silver and Gold layers do not exist yet** in code — only the Ozone buckets (`silver`, `gold`, `platinum`) are pre-created by bootstrap. You will be creating this layer.
- **Airflow and dbt folders exist but are not wired into `docker-compose.yml`** — no orchestration or transformation-as-SQL layer runs yet. Treat them as reserved space.
- The **producer** has two parallel code paths (see §3) — only one is actually used today.

---

## 2. Repository Overview

```
project_dhonuk/
├── producer/         # Generates / polls source data, publishes to Kafka
├── consumer/         # Reads Kafka, batches, writes to ClickHouse / Ozone / Postgres
├── spark/            # Batch/streaming jobs: Ozone (raw) → Iceberg (bronze/silver/gold)
├── trino/             # Trino catalog configuration (query layer over Iceberg)
├── bootstrap/         # One-shot setup scripts run at stack startup
├── clickhouse/        # ClickHouse init SQL and user config
├── images/            # Dockerfiles for each service (producer, consumer, spark, kafka-connect, airflow)
├── airflow/           # Reserved for future DAG-based orchestration (not yet active)
├── dbt/               # Reserved for future SQL-based transformations (currently empty)
└── docker-compose.yml # Defines and wires every service together
```

### `producer/`
- **Why it exists:** Owns everything about getting data *into* Kafka.
- **What belongs here:** Data generators, JDBC polling sources (WIP), Kafka publishing logic, and Kafka Connect connector configs (`connectors/*.json`) for the Oracle/Postgres pull path.
- **When it's used:** Runs continuously as the `producer` container (synthetic generator today); `connectors/register.sh` is run manually/once against `kafka-connect` to register real source polling.

### `consumer/`
- **Why it exists:** Owns everything about getting data *out* of Kafka and into a destination.
- **What belongs here:** Kafka consumer wrapper, the generic batching `Pipeline`, and one `Sink` implementation per destination (ClickHouse, Ozone, Postgres).
- **When it's used:** Runs continuously as the `consumer` container, and once (via the same image) as the `ozone-bootstrap` init job.

### `spark/`
- **Why it exists:** Owns all batch/streaming transformation logic that turns raw data into governed Iceberg tables (Bronze → Silver → Gold).
- **What belongs here:** Spark job classes (`src/jobs/`), shared readers/writers (`src/readers/`, `src/writers/`), job configuration (`src/config/pipelines.yaml`), and Spark/Iceberg/Ozone connection settings (`conf/spark-defaults.conf`).
- **When it's used:** Run manually today (`spark` container stays up via `tail -f /dev/null`; you `docker exec` in and run `python src/main.py` or `spark-submit`). Intended to eventually be scheduled by Airflow.

### `trino/`
- **Why it exists:** Configures the SQL query layer.
- **What belongs here:** Catalog property files only — no application code. One file per catalog (currently just `iceberg.properties`).
- **When it's used:** Read once at Trino container startup; changes require a Trino restart.

### `bootstrap/`
- **Why it exists:** Idempotent, run-once setup that other services assume has already happened.
- **What belongs here:** Scripts like `ozone_bootstrap.py`, which waits for the Ozone S3 gateway and ensures the `raw`, `bronze`, `silver`, `gold`, `platinum` buckets exist.
- **When it's used:** Runs once as the `ozone-bootstrap` container before `producer`/`consumer` start (they `depends_on` it).

### `clickhouse/`
- **Why it exists:** ClickHouse-specific bootstrapping.
- **What belongs here:** `init/` — SQL files auto-executed by the official image on first startup (e.g., `CREATE TABLE` statements for sinks referenced in `pipelines.yaml`); `users.d/` — user/access XML overrides.
- **When it's used:** Only on a fresh ClickHouse volume. Currently empty — tables referenced in `consumer/src/config/pipelines.yaml` (e.g. `analytics.transaction_data_streaming`) must exist beforehand, either created here or manually.

### `images/`
- **Why it exists:** One Dockerfile per service, kept out of each service's own folder so `docker-compose.yml`'s `context: .` can reach both the Dockerfile and the shared source tree.
- **What belongs here:** `images/<service>/Dockerfile` for `producer`, `consumer`, `spark`, `kafka-connect`, `airflow`.
- **When it's used:** At `docker compose build`.

### `airflow/` and `dbt/`
- **Why they exist:** Reserved space for future orchestration (Airflow DAGs to schedule Spark jobs) and future SQL-based transformation (dbt models for Silver/Gold). Not referenced by `docker-compose.yml` today.
- **What belongs here (eventually):** `airflow/dags/*.py` DAGs that trigger Spark jobs on a schedule; `dbt/` models/tests once Silver/Gold logic is expressed in SQL rather than PySpark.

---

## 3. Code Flow

### Producer

| File | Why it exists | Who calls it | What it does | Depended on by |
|---|---|---|---|---|
| `producer/src/main.py` | **The actual current entrypoint.** Runs the container's `python /app/src/main.py`. | Docker (`command:` in compose) | Loops forever: generate a `Transaction`, publish it to the default topic *and* to `transaction_stream`, sleep, repeat. | `generator.py`, `publishers/factory.py`, `config.py` |
| `producer/src/generator.py` | Produces fake but realistic transaction records for dev/demo. | `main.py`, `sources/generator_source.py` | `TransactionGenerator.generate()` returns a random `Transaction` dataclass instance. | `models.py` |
| `producer/src/models.py` | Defines the `Transaction` schema shared by generator and (indirectly) every downstream consumer/sink column list. | `generator.py` | Dataclass with `to_dict()` / `to_json()`. **This is the canonical shape of a transaction record** — any field added/removed here must be mirrored in `consumer/src/config/pipelines.yaml` `columns:` lists and ClickHouse table DDL. | `publishers/kafka.py`, `publishers/console.py` |
| `producer/src/publishers/factory.py` | Chooses a publisher implementation by name (`"kafka"` or `"console"`). | `main.py`, `registry.py` | Returns a `Publisher` instance. | `publishers/kafka.py`, `publishers/console.py` |
| `producer/src/publishers/kafka.py` | Talks to Kafka. | `factory.py` | Waits for Kafka, ensures the topic exists (creates with 3 partitions if missing), produces messages, flushes on close. | confluent-kafka |
| `producer/src/config.py` | Simple env-var config for the *current* single-source path (`main.py`). | `main.py`, `publishers/factory.py`, `registry.py` | Reads `EVENTS_PER_SECOND`, `PUBLISHER`, `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `LOG_LEVEL` from env. | — |

**⚠️ Not currently wired in (multi-source framework, in progress):**
`producer/src/manager.py`, `registry.py`, `sources/base.py`, `sources/factory.py`, `sources/generator_source.py` implement a more general "many named sources → many topics" architecture (`build_pipelines()` + `run_all()`, driven by a not-yet-created `sources.yaml`-style config). `sources/factory.py` also references `sources.jdbc.JDBCPollingSource`, which **does not exist yet** — this path will `ImportError` if invoked. Real JDBC polling today is instead handled entirely outside Python, by **Kafka Connect** (`producer/connectors/*.json` + `kafka-connect` service). Treat `manager.py`/`registry.py`/`sources/` as an in-progress refactor target, not dead code to delete — see §5 for how it's meant to be finished.

### Consumer

| File | Why it exists | Who calls it | What it does | Depended on by |
|---|---|---|---|---|
| `consumer/src/main.py` | Entrypoint. | Docker | Loads config, builds pipelines, starts them all. | `config/config.py`, `registry.py`, `manager.py` |
| `consumer/src/config/config.py` | Loads `application.yaml` (connections) and `pipelines.yaml` (topic → sink mappings) with `${VAR}` env substitution. | `main.py` | Returns two plain dicts. | — |
| `consumer/src/registry.py` | **The wiring point.** Turns each `pipelines.yaml` entry into a running `PipelineRunner` (consumer + sink + batching config). | `main.py` | For each enabled, non-`spark`-typed pipeline: builds a `Sink` (merging `application.yaml` connection config with the pipeline's own sink block), wraps it in a `Pipeline`, and pairs it with a `KafkaConsumerClient`. | `sinks/factory.py`, `pipeline.py`, `consumers/kafka_consumer.py` |
| `consumer/src/manager.py` | Runs every pipeline concurrently, one thread each. | `main.py` | Poll → buffer → flush-when-ready → commit offset **only after a successful sink write**. If a pipeline thread dies, the whole process exits (`os._exit(1)`) so the orchestrator restarts it — deliberate fail-fast design. | `registry.py` |
| `consumer/src/pipeline.py` | Generic batching logic, shared by every sink type. | `manager.py`, `registry.py` | Buffers JSON-decoded records, converts configured `datetime_fields` to `datetime` objects, decides `is_ready()` by size/time/overflow thresholds, and `flush()`es to the sink. | `sinks/base.py` |
| `consumer/src/sinks/factory.py` | Chooses a `Sink` implementation by type string from `pipelines.yaml`. | `registry.py` | Returns a configured `Sink`. | `sinks/clickhouse.py`, `sinks/ozone.py`, `sinks/postgres.py` |
| `consumer/src/sinks/clickhouse.py` | Writes batches into ClickHouse. | `factory.py` | Maps each record's configured `columns` to a row and inserts with retry/backoff. | `clickhouse-connect` |
| `consumer/src/sinks/ozone.py` | Writes batches as NDJSON files into Ozone (S3), partitioned by `year/month/day/hour`. **This is what produces the raw layer Spark reads.** | `factory.py` | Builds a time-partitioned S3 key, joins records as newline-delimited JSON, `put_object`s with retry/backoff. | `boto3` |
| `consumer/src/sinks/postgres.py` | Placeholder — `write()` raises `NotImplementedError`. | `factory.py` (if `pipelines.yaml` enables a `postgres` sink) | Nothing yet — implement when a Postgres pipeline is actually needed. | — |
| `consumer/src/consumers/kafka_consumer.py` | Thin wrapper around `confluent_kafka.Consumer`. | `registry.py`, `manager.py` | Subscribe, poll, manual commit, close. `enable.auto.commit=False` is intentional — offsets are only committed by `manager.py` after a successful flush. | confluent-kafka |

### Spark

| File | Why it exists | Who calls it | What it does | Depended on by |
|---|---|---|---|---|
| `spark/src/main.py` | Entrypoint for `python src/main.py` inside the `spark` container. | You (manually, via `docker exec`) | Instantiates and runs `SparkManager`. | `manager.py` |
| `spark/src/manager.py` | Reads `pipelines.yaml`, and for every enabled job, looks it up in the registry, instantiates it with its config block, and runs it. | `main.py` | Iterates `config.config.pipelines()`. | `registry.py`, `config/config.py` |
| `spark/src/registry.py` | Maps a job **name** (from `pipelines.yaml`) to a job **class**. | `manager.py` | `{"raw_to_bronze": RawToBronzeJob}` today — this is the extension point for new jobs (§6). | `jobs/raw_to_bronze.py` |
| `spark/src/config/config.py` | Loads `application.yaml` (Spark/catalog defaults) and `pipelines.yaml` (per-job source/sink config) as plain YAML — no env substitution here (unlike producer/consumer). | `manager.py` | `application()` / `pipelines()`. | — |
| `spark/src/jobs/raw_to_bronze.py` | The one real job today. **Currently a skeleton**: builds a `SparkSession` with Iceberg/Polaris/Ozone config hardcoded inline, prints the Spark version, and stops. Does not yet read Ozone or write Iceberg. | `manager.py` | See above. | `pyspark`, Iceberg/AWS jars in `spark/jars/` |
| `spark/src/readers/ozone.py` | **Empty stub.** Intended home for a reusable "read raw NDJSON from Ozone as a DataFrame" function. | (not yet called) | — | — |
| `spark/src/writers/iceberg.py` | **Empty stub.** Intended home for a reusable "write/merge a DataFrame into an Iceberg table via the Polaris catalog" function. | (not yet called) | — | — |

### Other flow-relevant files

- `bootstrap/ozone_bootstrap.py` — waits for the Ozone S3 gateway, then creates the `raw`, `bronze`, `silver`, `gold`, `platinum` buckets if missing. Every other service that touches Ozone depends on this running first.
- `producer/connectors/register.sh` — a manual utility, not run automatically. Reads each `producer/connectors/*.json`, and PUTs its `config` block to the Kafka Connect REST API to register/update a JDBC source connector.
- `trino/catalog/iceberg.properties` — no code, but functionally the file that makes Bronze/Silver/Gold tables (registered in Polaris) visible to SQL clients.

---

## 4. Configuration Guide

| File | Purpose | What breaks if it's wrong |
|---|---|---|
| `docker-compose.yml` | Wires every service, network, volume, port, and startup dependency together. `x-producer-common` / `x-consumer-common` anchors share env/network/depends_on across services using the same image. | Wrong `depends_on` ordering → services fail to connect on cold start (e.g. producer trying to publish before `ozone-bootstrap` created buckets). Wrong port mapping → other tools (Trino UI, CloudBeaver, Kafka UI) become unreachable from the host. |
| `.env` | Root-level environment variables consumed by `docker-compose.yml` interpolation (`${VAR:-default}`) and passed into containers. | Missing/incorrect Kafka topic name, ClickHouse/Postgres credentials, or Ozone credentials here silently fall back to defaults baked into compose — can cause containers to connect to the wrong topic/db without an obvious error. |
| `producer/src/config.py` / `consumer/src/config/application.yaml` | Runtime connection settings for the *currently active* code paths (simple producer loop; consumer pipelines). | Wrong `KAFKA_BOOTSTRAP_SERVERS`/`bootstrap_servers` → producer/consumer can't reach Kafka at all (retries then crashes for consumer; producer's `_wait_for_kafka` retries then raises). |
| `consumer/src/config/pipelines.yaml` | Declares every consumer pipeline: topic, sink type, sink connection params, batching thresholds, and — critically — the exact `columns` list used to map JSON fields to sink columns. | A `columns` entry that doesn't exist on the source record silently inserts `NULL`/missing values (uses `.get()`); a ClickHouse `table` that doesn't exist yet causes every insert to fail and the whole pipeline thread to crash (by design — see manager.py). |
| `spark/src/config/application.yaml` | Spark app name, master, log level, checkpoint root, and default catalog (`polaris`) connection info. | Wrong `catalog.uri`/`warehouse` → Spark can't resolve or create Iceberg tables in Polaris. Note: **`raw_to_bronze.py` currently hardcodes these same values instead of reading this file** — if you change this YAML expecting it to take effect, it won't until the job is refactored to actually consume it (see §6). |
| `spark/src/config/pipelines.yaml` | Declares Spark jobs: name, `source` (type/path/format), `sink` (namespace/table), trigger interval. Read by `manager.py`/`registry.py` to decide which job classes to instantiate. | An unregistered `name` → `JobRegistry.get()` raises `ValueError`. A `namespace`/`table` that doesn't yet exist in Polaris → job must create it (not automatic). |
| `spark/conf/spark-defaults.conf` | Cluster-wide Spark defaults: Iceberg extension registration, the `polaris` catalog definition (REST URI, OAuth credential, warehouse), and S3A settings for reading raw Ozone data. This is the file `spark-submit`/most real jobs should rely on instead of hardcoding config in Python. | Missing/incorrect `spark.sql.catalog.polaris.*` → any `spark.sql("... polaris.bronze...")` or DataFrame write to the `polaris` catalog fails immediately. Wrong `s3.endpoint`/credentials → can't read or write any Ozone-backed data. |
| `trino/catalog/iceberg.properties` | Trino's `iceberg` catalog: same Polaris REST URI/credential/warehouse as Spark, plus native S3 settings pointing at Ozone. | Any mismatch with Polaris's actual OAuth credential or Ozone endpoint → Trino queries against `iceberg.*` tables fail, even though the tables exist and are valid. |
| `bootstrap/ozone_bootstrap.py` (its `REQUIRED_BUCKETS` list) | Declares which top-level Ozone buckets must exist before anything else runs. | Adding a new logical layer (e.g. a new bucket) without adding it here means it's never auto-created, so anything writing to it fails on a fresh environment. |
| `images/kafka-connect/Dockerfile` + `producer/connectors/*.json` | Installs the JDBC source connector plugin and PostgreSQL driver at build time; the JSON files are the actual per-table connector configs registered via `register.sh`. | Oracle driver isn't downloadable/redistributable — must be placed manually in `producer/connect-drivers/` (currently not even mounted in compose — see the commented-out `volumes:` line under `kafka-connect`). Without it, the Oracle connector will fail to register/run. |
| `polaris` env block in `docker-compose.yml` | Configures Polaris's own Postgres-backed persistence and its AWS-style credentials for talking to Ozone as S3. | Wrong Postgres connection → Polaris fails to start / loses catalog state. Wrong AWS creds → Polaris can still serve metadata but any storage-credential-vending flow to Ozone breaks. |

**Credential note:** `spark-defaults.conf` and `trino/catalog/iceberg.properties` both hardcode the same Polaris OAuth `credential=root:s3cr3t`. Keep any catalog client (new Spark job, new Trino catalog, a future dbt profile) in sync with this value, or better, move it into `.env` / secrets management before this goes beyond local dev.

---

## 5. How to Add a New Pipeline

Example: tomorrow you receive a new Oracle source table.

```
New Oracle Table
        ↓
Kafka Connect connector config (producer/connectors/*.json)
        ↓
Kafka Topic (auto-created, e.g. "oracle.SCHEMA.NEW_TABLE")
        ↓
Consumer pipeline (consumer/src/config/pipelines.yaml)
        ↓
Raw Storage in Ozone (OzoneSink, if you want a raw/bronze path)
        ↓
Bronze Spark Job (spark/src/config/pipelines.yaml + a job class)
        ↓
Silver Transformation (new job — doesn't exist yet, follow §6)
        ↓
Gold Datamart (new job — doesn't exist yet, follow §6)
```

**Step-by-step, with exact files:**

1. **Register the source with Kafka Connect.**
   - Copy `producer/connectors/oracle-source.json` (or `postgres-source.json`) as a template.
   - Set `connection.url`, `query` (or `table.whitelist`), `timestamp.column.name` (for incremental polling), and a distinct `transforms.RenameTopic.replacement` so the new table lands on its own topic.
   - If it's Oracle and you haven't already, download `ojdbc11.jar` into `producer/connect-drivers/` and uncomment the matching `volumes:` line for `kafka-connect` in `docker-compose.yml`.
   - Run `producer/connectors/register.sh` (or `PUT` it manually) against the running `kafka-connect` service.

2. **Confirm the topic.** Kafka Connect auto-creates the topic on first poll; verify it in Kafka UI (`localhost:8081`).

3. **Add a consumer pipeline.** In `consumer/src/config/pipelines.yaml`, add a new entry:
   - `topic`: the topic from step 1.
   - `sink.type: ozone` to land raw JSON (recommended for anything that will feed Bronze), and/or `sink.type: clickhouse` if you also want fast operational access.
   - `datetime_fields` for any timestamp columns.
   - Give it its own `consumer_group` if it should be independent from existing pipelines.
   - No code changes needed here — `registry.py` builds this automatically from YAML.

4. **(If sinking to ClickHouse) create the destination table.** Add a `CREATE TABLE` statement to `clickhouse/init/` (only applies to a fresh volume) or run it manually — the `ClickHouseSink` does not create tables for you.

5. **Add a Bronze Spark job entry.** In `spark/src/config/pipelines.yaml`, add a job pointing `source.path` at the new Ozone prefix and `sink.namespace/table` at the new Bronze table name.

6. **Implement (or extend) the job class.** See §6 for the recommended pattern — today this means finishing `RawToBronzeJob` (or a new job class) to actually read from `readers/ozone.py` and write via `writers/iceberg.py`, and registering it in `spark/src/registry.py` if it's a new class name.

7. **Design Silver/Gold as new job classes** once Bronze is flowing — same registry pattern, reading from the `polaris` catalog's `bronze` namespace and writing to `silver`/`gold`.

8. **Validate in Trino.** Query `iceberg.bronze.<table>` (and later `silver`/`gold`) — no Trino config changes are needed as long as the table is registered in the same Polaris catalog.

---

## 6. How to Add a New Spark Job

**Recommended structure (extends what's already there):**

```
spark/src/
├── jobs/
│   ├── raw_to_bronze.py       # existing
│   ├── bronze_to_silver_<x>.py   # one file per Silver transformation
│   └── silver_to_gold_<x>.py     # one file per Gold datamart
├── readers/
│   └── ozone.py               # shared "read raw/bronze from Ozone/Iceberg" helpers
├── writers/
│   └── iceberg.py             # shared "write/merge into Iceberg via Polaris" helpers
├── config/
│   ├── application.yaml       # shared Spark/catalog settings
│   └── pipelines.yaml         # one entry per job, keyed by job name
├── registry.py                 # name → class map
└── manager.py                  # iterates pipelines.yaml, runs enabled jobs
```

**Where new jobs go:** one class per file under `spark/src/jobs/`, named after what it produces (`bronze_to_silver_transactions.py`, not `job2.py`).

**How they should be organized (target shape for each job class):**

```python
class BronzeToSilverTransactions:
    def __init__(self, config: dict):
        self.config = config  # the matching pipelines.yaml entry

    def run(self):
        spark = get_spark_session()          # shared helper, NOT rebuilt per job
        df = read_iceberg(spark, "polaris.bronze.transaction_stream")
        result = self._transform(df)
        write_iceberg(spark, result, "polaris.silver.transactions", mode="merge")
        spark.stop()

    def _transform(self, df):
        ...  # the only part that's actually job-specific
```

**How configuration should be reused:**
- Move the Spark/Iceberg/S3 config currently hardcoded inline in `raw_to_bronze.py` into a single shared `get_spark_session()` helper (e.g. in a new `spark/src/session.py`), built from `spark/src/config/application.yaml` and/or `conf/spark-defaults.conf` — don't repeat the `.config(...)` chain in every job file. Today it's duplicated logic waiting to be extracted.
- Prefer `conf/spark-defaults.conf` for anything static (Polaris URI, S3 endpoint, credentials) — it's loaded automatically by the Spark distribution — and reserve `application.yaml`/`pipelines.yaml` for things that vary per job (source path, sink table, batch vs. streaming, trigger interval).

**How jobs should read from Raw/Bronze:**
- Raw (Ozone, NDJSON): implement `read_raw(spark, path, format="json")` in `readers/ozone.py`, wrapping `spark.read.format(...).load(f"s3a://raw/{path}")`.
- Bronze/Silver (Iceberg): read via the catalog, not the filesystem: `spark.read.table("polaris.bronze.transaction_stream")` (or `spark.sql("SELECT * FROM polaris.bronze.transaction_stream")`) — this is what makes Polaris the single source of truth for table location/schema instead of hardcoded paths.

**How jobs should write to Iceberg:**
- Implement `write_iceberg(df, table, mode)` in `writers/iceberg.py` wrapping `df.writeTo("polaris.<namespace>.<table>")`, supporting at least `create` (first run) and `append`/`overwritePartitions`/`merge` (subsequent runs) so re-running a job is safe.
- Always target the `polaris` catalog explicitly (`polaris.bronze.x`, `polaris.silver.x`) so table location and metadata stay governed centrally.

**Wiring a new job in:**
1. Write the class under `spark/src/jobs/`.
2. Add it to `spark/src/registry.py`'s `_jobs` dict.
3. Add an entry to `spark/src/config/pipelines.yaml` with `enabled: true` and its source/sink config.
4. Run it: `docker exec -it spark python src/main.py` (runs every enabled job) — for one job at a time during development, temporarily disable the others in `pipelines.yaml`.

---

## 7. What to Prepare Before Processing Data

Checklist before building a new Bronze or Silver table:

- [ ] **Raw data location** — confirm the Ozone bucket/prefix exists and is actually receiving data (check via `consumer/src/tests/test_ozone_file_count.py` style listing, or Ozone Recon UI on `:9888`).
- [ ] **Ozone bucket** — is it one of `bootstrap/ozone_bootstrap.py`'s `REQUIRED_BUCKETS`? If not, add it there so it survives environment rebuilds.
- [ ] **Iceberg namespace** — does the target namespace (`bronze`, `silver`, `gold`, `platinum`) exist in Polaris? Create with `spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.<namespace>")` or via Polaris's own API/CLI.
- [ ] **Table definition** — decide the target schema up front (types, nullability) rather than letting Spark infer it from JSON on every run — inferred schemas can drift silently between batches.
- [ ] **Partition strategy** — decide partition columns (commonly by ingestion date, or a business date column) before first write; changing Iceberg partitioning later is possible but adds operational complexity.
- [ ] **Catalog configuration** — is `polaris` correctly defined in `conf/spark-defaults.conf` (and mirrored in `trino/catalog/iceberg.properties` if it needs to be queryable)?
- [ ] **Required Spark configuration** — Iceberg + AWS jars present under `spark/jars/` (mounted as `spark/jars/custom` in compose) and referenced consistently — don't hardcode a jar list in a job file that diverges from `spark-defaults.conf`.
- [ ] **Required permissions** — Ozone/S3 credentials (`ozone`/`ozone` today) valid for both read (raw bucket) and write (bronze/silver/gold buckets); Polaris principal/role (`PRINCIPAL_ROLE:ALL` today) has rights to create tables in the target namespace.
- [ ] **Idempotency plan** — decide whether the job appends, upserts (merge), or overwrites a partition on re-run, *before* writing the job — this determines whether re-running after a failure duplicates data.

---

## 8. Best Practices

- **Keep Spark jobs modular.** One job = one class = one file under `jobs/`. Shared logic (session creation, reading, writing) belongs in `readers/`/`writers`/a session helper, not copy-pasted per job — `raw_to_bronze.py` currently hardcodes its own session config; don't repeat that pattern for new jobs.
- **Naming conventions.**
  - Kafka topics: `<source>.<schema>.<table>` for JDBC-sourced topics (matches the existing `oracle.BI_REPORTS.IAS_DFS_TXN_LOG` pattern); short business names (`txn_events`, `transaction_stream`) for synthetic/internal topics.
  - Spark job names in `pipelines.yaml`/`registry.py`: `<source_layer>_to_<target_layer>[_<entity>]`, e.g. `raw_to_bronze`, `bronze_to_silver_transactions`.
  - Iceberg tables: `<namespace>.<entity>`, singular/plural consistent with existing tables (`bronze.transaction_stream`).
- **Folder organization.** Mirror the producer/consumer pattern already in place: an abstract `base.py`, one implementation file per concrete type, and a `factory.py` that maps a config string to a class. Apply the same shape to any new Spark reader/writer/source type.
- **Configuration management.** Business/environment-specific values (topic names, table names, credentials, batch sizes) belong in YAML (`pipelines.yaml`, `application.yaml`) or `.env` — not hardcoded in Python. Anything that must match across services (e.g. the Polaris OAuth credential in both `spark-defaults.conf` and `iceberg.properties`) is a good candidate to eventually source from one shared `.env` value.
- **Logging.** Follow the existing pattern: `logging.getLogger(__name__)`, structured messages with a `[pipeline_name]` prefix, `logger.exception(...)` inside `except` blocks so tracebacks are preserved. Avoid bare `print()` outside of throwaway scripts (`ConsolePublisher` is an intentional, documented exception).
- **Error handling.** Preserve the existing "fail fast, let the orchestrator restart" philosophy: a pipeline/job thread that hits an unrecoverable error should raise and let the process exit, rather than swallowing the error and silently doing nothing. Sinks should raise on failure *before* the caller commits any offset or marks anything complete (see `Sink.write` docstring, `Pipeline.flush`).
- **How to avoid breaking existing pipelines.**
  - Never remove or rename a `Transaction` field without checking every `columns:` list in `consumer/src/config/pipelines.yaml` and every downstream ClickHouse/Iceberg schema that references it.
  - Add new pipelines as new YAML entries with `enabled: true`; don't repurpose an existing entry, so you can disable your addition (`enabled: false`) without touching working pipelines.
  - Treat `spark/src/config/pipelines.yaml` job names as stable identifiers — `registry.py` looks them up by exact string.
  - When changing shared config (`spark-defaults.conf`, `application.yaml`), test against the specific job you're changing before assuming it applies everywhere — remember `raw_to_bronze.py` currently ignores `application.yaml` and hardcodes its own settings.

---

## 9. Suggested Development Workflow

```
Requirement
    ↓
Design (which layer changes: new source? new sink? new transform?)
    ↓
Producer / Kafka Connect change (if new source)
    ↓
Consumer pipeline change (pipelines.yaml + sink, if needed)
    ↓
Raw validation (confirm NDJSON landing in Ozone, correct partitioning)
    ↓
Bronze Spark job (read raw → write Iceberg bronze)
    ↓
Silver Spark job (cleanse/conform bronze → silver)
    ↓
Gold Spark job (aggregate/model silver → gold datamart)
    ↓
Trino validation (query the new table(s) end-to-end)
    ↓
Deployment (docker compose build/up affected services; register new Kafka Connect connectors; re-run bootstrap if new buckets/namespaces were added)
```

**Practical notes for this specific repo:**
- Most iteration on Spark jobs happens by `docker exec -it spark bash` and running `python src/main.py` (or a `spark-submit`) directly — there's no CI/test harness for Spark jobs yet, so validate by querying Trino afterward.
- The `consumer` and `producer` containers hot-reload code because `./producer/src` / `./consumer/src` are bind-mounted — no rebuild needed for Python-only changes, only `docker compose restart producer|consumer`. A rebuild (`docker compose build`) is only required when `requirements.txt` or the Dockerfile changes.
- Before calling a pipeline "done," confirm the full chain: Kafka UI shows messages on the topic → consumer logs show batches flushing → Ozone/ClickHouse shows the data → (for Bronze+) Trino can `SELECT` it.
