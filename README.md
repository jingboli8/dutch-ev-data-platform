# Dutch EV Data Platform

A privacy-aware local data engineering project that turns official Dutch RDW open
data into typed DuckDB and Parquet analytical models. Phase 1 provides scalable,
bounded-memory, resumable snapshot ingestion using Python, Requests, DuckDB, and
pandas.

The project intentionally does not include Azure, Spark, Kafka, Docker, dbt,
Power BI, invented performance claims, or claimed business impact.

## Capabilities

- Pages through a configurable number of EVs or the complete qualifying source.
- Uses deterministic keyset pagination over grouped `kenteken` values.
- Retrieves matching vehicle rows and complete fuel profiles in bounded batches.
- Persists content-addressed raw API pages for audit and safe resume.
- Replaces licence plates with salted SHA-256 hashes before staging.
- Checkpoints each completed page without storing plaintext vehicle identifiers.
- Resumes interrupted snapshots from the last completed page.
- Idempotently upserts staging data and audits duplicate source payloads.
- Produces EV metrics by fuel, brand, model, and registration year.
- Exports privacy-safe staging and analytical tables to compressed Parquet.
- Records extraction, throughput, checkpoint, and data-quality metrics.

## Data sources

- [Open Data RDW: Registered vehicles](https://opendata.rdw.nl/Voertuigen/Open-Data-RDW-Gekentekende_voertuigen/m9d7-ebf2)
  (`m9d7-ebf2`)
- [Open Data RDW: Registered vehicle fuels](https://opendata.rdw.nl/Voertuigen/Open-Data-RDW-Gekentekende_voertuigen_brandstof/8ys7-d773)
  (`8ys7-d773`)

The source is accessed through the official Socrata resource endpoints configured
in `config/settings.toml`.

## Architecture

```text
RDW fuel dataset
  grouped EV identifiers ordered by kenteken
             |
             | keyset pages
             v
Python snapshot orchestrator
  one identifier page in memory
             |
             +--> bounded matching vehicle queries
             +--> bounded complete fuel-profile queries
             +--> content-addressed data/raw/*.json
             |
             v
normalize + salted SHA-256 identifier
             |
             v
DuckDB
  meta.ingestion_runs
  meta.ingested_payloads
  staging.vehicles
  staging.fuels
  analytics.ev_vehicles
  analytics.ev_fuel_details
  analytics.ev_metrics
             |
             v
data/parquet/*.parquet
```

Only one EV identifier page and its matching detail rows are held in memory. Each
page is normalized and upserted before the next page is requested.

## Pagination and resume design

The fuel dataset is filtered to `Elektriciteit` or `Waterstof`, grouped by
`kenteken`, and ordered by `kenteken`. Subsequent pages use
`kenteken > last_completed_key`, which avoids the increasing query cost and row
shifts associated with large offsets.

The key itself is never written to a checkpoint. The checkpoint stores only the
SHA-256 digest of the last completed raw identifier page. On resume, the key is
derived from that Git-ignored raw page. Therefore:

- checkpoints contain no plaintext licence plates;
- a resume requires both the checkpoint and its referenced private raw page;
- an interrupted page may be requested again, but staging upserts and payload
  hashes make the operation safe and auditable;
- a page that does not advance the key is rejected to prevent an infinite loop.

Raw pages and checkpoint files are atomically replaced. Each referenced raw page
is verified against its SHA-256 digest before a cursor is recovered. A checkpoint
also stores a privacy-safe configuration fingerprint, so a resume must use the
same source endpoints, data and warehouse locations, limit, page size, detail
batch size, and hash salt. Request timeout, retry, and log-level changes are safe.

## Snapshot versus true incremental ingestion

This implementation is a **resumable snapshot ingestion**, not a true
source-incremental pipeline.

The published RDW schemas do not expose a reliable row-level update timestamp or
change sequence across both source datasets. Socrata's dataset-level
`rowsUpdatedAt` metadata indicates that a publication changed, but it cannot
identify changed rows, detect deletions, or serve as a row filter. Business dates
such as first registration dates and the vehicle type-approval change sequence
are not update watermarks.

Consequently, the pipeline deliberately does not label snapshot reruns as
incremental loads. True incremental ingestion would require an authoritative
row-level change timestamp, change-data feed, or versioned source export.

## Consistency limitations

Keyset pagination is deterministic and resilient to changes before the cursor,
but the public API does not provide a transactionally frozen snapshot:

- records inserted below an already processed key can be missed during that run;
- source rows can be updated or deleted between identifier and detail requests;
- matching vehicle and fuel rows are retrieved at slightly different times;
- the unauthenticated Socrata API can throttle large request volumes.

Run metadata makes these limitations observable, but cannot eliminate them.
For a production-grade historical snapshot, the source would need a versioned
bulk export or snapshot-isolation capability.

## Setup

PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
Copy-Item .env.example .env
.\.venv\Scripts\python.exe -m pytest
```

All dependencies are intentional:

- `requests`: reliable HTTP timeouts, response validation, and retry handling;
- `duckdb`: local warehouse, SQL transformations, quality checks, and Parquet;
- `pandas`: page-sized typed DataFrame registration with DuckDB;
- `pytest` (development only): automated validation.

Phase 1 adds no new dependency.

## Running snapshots

Exactly one mode must be selected:

- `--fresh`: start a new snapshot and clear the current staging snapshot;
- `--resume`: continue the last interrupted checkpoint.

Small validation:

```powershell
.\.venv\Scripts\dutch-ev.exe --fresh --limit 500 --page-size 250
```

Larger bounded run:

```powershell
.\.venv\Scripts\dutch-ev.exe --fresh --limit 10000 --page-size 1000
```

Resume the same run after an interruption:

```powershell
.\.venv\Scripts\dutch-ev.exe --resume --limit 10000 --page-size 1000
```

Complete qualifying snapshot:

```powershell
.\.venv\Scripts\dutch-ev.exe --fresh --limit 0 --page-size 1000
```

`--limit 0` means no limit. `--detail-batch-size` optionally controls how many
identifiers are included in each matching detail query; its configured default is
conservative enough to avoid very long URLs.

Configuration can be overridden without editing source:

| Environment variable | Purpose | Default |
|---|---|---:|
| `EV_SNAPSHOT_LIMIT` | Matched EV cap; `0` means complete | `10000` |
| `EV_API_PAGE_SIZE` | Identifier and detail response page size | `1000` |
| `EV_DETAIL_BATCH_SIZE` | Identifiers per matching detail query | `200` |
| `EV_REQUEST_TIMEOUT_SECONDS` | HTTP request timeout | `30` |
| `EV_DATA_DIR` | Raw and Parquet root | `data` |
| `EV_DATABASE_PATH` | DuckDB warehouse | `data/warehouse/dutch_ev.duckdb` |
| `EV_STATE_DIR` | Private checkpoint and salt root | `.state` |
| `EV_HASH_SALT` | Optional stable private hash salt | generated locally |
| `EV_LOG_LEVEL` | Structured JSON log threshold | `INFO` |

## Operational metadata

`meta.ingestion_runs` records:

- completed logical identifier pages and total API page requests;
- source rows received across identifier, vehicle, and fuel responses;
- matched vehicle rows and accepted fuel rows;
- rejected or unmatched rows;
- duplicate content-addressed payloads;
- active processing duration, wall-clock elapsed time, and processed rows per
  second;
- checkpoint status, resume flag, and resume count;
- failure details for interrupted runs.

`processed_rows_per_second` is calculated from accepted vehicle rows plus
accepted fuel rows divided by `active_duration_seconds`. Active duration sums
time spent in pipeline invocations and excludes downtime between interruption and
resume. `wall_clock_elapsed_seconds` measures elapsed time from the original
snapshot start through the latest metadata update, including downtime. These are
observed operational metrics, not benchmarks.

`meta.ingested_payloads` registers each unique dataset/payload SHA-256 pair.
Repeated source content is counted as a duplicate and does not create another raw
file.

## Data model

`staging.vehicles` contains typed vehicle attributes and one salted
`vehicle_id_hash` per vehicle. `staging.fuels` contains typed fuel attributes,
keyed by the same hash and fuel sequence.

`analytics.ev_vehicles` contains one row per electric or hydrogen vehicle.
`analytics.ev_fuel_details` retains all fuels for those vehicles so hybrid
profiles remain visible. `analytics.ev_metrics` aggregates vehicle count, average
reported combined CO2, and average net maximum power by fuel type, brand, model,
and registration year.

Example:

```sql
SELECT
    fuel_type,
    brand,
    model,
    registration_year,
    vehicle_count
FROM analytics.ev_metrics
ORDER BY vehicle_count DESC
LIMIT 50;
```

Additional queries are available in `sql/analytics_examples.sql`.

## Privacy

Licence plates are temporarily required to join the public APIs. They may exist
only in process memory and the local raw zone:

- `data/`, `.state/`, `.env*`, DuckDB, Parquet, and logs are Git-ignored;
- raw API pages are content-addressed and remain private;
- the real salt is generated under `.state/privacy_salt` unless supplied through
  the environment;
- checkpoint JSON stores digests and counters, never licence plates;
- staging, analytics, logs, and Parquet use only salted hashes;
- structured logs contain counts and operational state, not row identifiers.

Do not publish raw files, the warehouse, checkpoints, or the salt. Production use
would additionally require access controls, retention rules, managed secrets, and
a formal privacy assessment.

## Tests

```powershell
.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\python.exe -m pip check
```

Tests cover keyset and detail pagination, exact partial limits, empty pages,
bounded retries, multiple logical pages, interruption and resume across durable
write boundaries, incompatible resume settings, corrupt raw anchors,
duplicate/non-advancing pages, duplicate payload auditing, idempotent fresh
reruns, checkpoint privacy, warehouse privacy, Parquet privacy, transformations,
and data-quality rules.
