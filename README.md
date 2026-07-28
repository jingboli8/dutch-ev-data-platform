# Dutch EV Data Platform

A small, independent data engineering portfolio project that turns official Dutch
RDW open data into a local, privacy-conscious analytical layer. The MVP is designed
for transparent local execution: Python handles extraction and orchestration,
DuckDB provides the warehouse, and Parquet provides portable processed outputs.

## What the MVP does

- Downloads a configurable, deterministic sample of EV identifiers from the fuel
  dataset and retrieves their matching registered-vehicle records.
- Retrieves all fuel records belonging to those sampled EVs, including secondary
  fuels needed to identify hybrid vehicles.
- Preserves exact API response arrays as immutable JSON files.
- Normalizes types and replaces licence plates with salted SHA-256 identifiers.
- Builds persistent `staging`, `analytics`, and `meta` schemas in DuckDB.
- Identifies battery-electric, hybrid-electric, and hydrogen-electric vehicles.
- Produces aggregate metrics by fuel type, brand, model, and registration year.
- Exports staging and analytical tables as compressed Parquet files.
- Audits every run and avoids duplicate staging records and payload registration.
- Runs automated extraction, transformation, pipeline, and privacy tests.

This project intentionally does not include cloud infrastructure, Spark, Kafka,
Docker, dbt, or BI tooling. It makes no performance or business-impact claims.

## Architecture

```text
RDW Socrata APIs
        |
        v
Python extractor (limited EV identifier sample + matching vehicles/fuels)
        |
        +--> data/raw/{vehicles,fuels}/*.json
        |       Exact source payloads; private local zone
        |
        v
Normalization + salted vehicle identifier hashing
        |
        v
DuckDB
  meta.ingestion_runs / meta.ingested_payloads
  staging.vehicles / staging.fuels
  analytics.ev_vehicles / ev_fuel_details / ev_metrics
        |
        v
data/parquet/*.parquet
```

The fuel dataset is filtered to electricity or hydrogen and ordered by `kenteken`
to obtain a repeatable, bounded EV sample. Matching vehicle rows and complete fuel
profiles are then requested in bounded identifier chunks. This guarantees joinable
samples and avoids the risk that a small generic vehicle sample contains no EVs.

## Official data sources

- [Open Data RDW: Registered vehicles](https://opendata.rdw.nl/Voertuigen/Open-Data-RDW-Gekentekende_voertuigen/m9d7-ebf2)
  (`m9d7-ebf2`)
- [Open Data RDW: Registered vehicle fuels](https://opendata.rdw.nl/Voertuigen/Open-Data-RDW-Gekentekende_voertuigen_brandstof/8ys7-d773)
  (`8ys7-d773`)

The pipeline uses the official Socrata resource endpoints configured in
`config/settings.toml`.

## Local setup

PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
Copy-Item .env.example .env
.\.venv\Scripts\dutch-ev.exe --limit 500
.\.venv\Scripts\python.exe -m pytest
```

The `.env` file is optional. On the first run, the pipeline creates a persistent
random salt at `.state/privacy_salt` unless `EV_HASH_SALT` is configured. Both
locations are ignored by Git. Key settings:

| Environment variable | Purpose | Default |
|---|---|---:|
| `EV_SAMPLE_LIMIT` | Maximum sampled EV identifiers | `500` |
| `EV_REQUEST_TIMEOUT_SECONDS` | HTTP timeout | `30` |
| `EV_API_PAGE_SIZE` | Fuel query row limit per chunk | `1000` |
| `EV_DATA_DIR` | Raw and Parquet root | `data` |
| `EV_DATABASE_PATH` | DuckDB warehouse path | `data/warehouse/dutch_ev.duckdb` |
| `EV_HASH_SALT` | Stable secret used to hash licence plates | generated locally |
| `EV_LOG_LEVEL` | Structured JSON log threshold | `INFO` |

## Data model

`meta.ingestion_runs` records run status, timestamps, requested sample size, row
counts, errors, and duplicate payload counts. `meta.ingested_payloads` identifies
source payloads by SHA-256 digest. Raw files are content-addressed, so re-fetching
identical source content neither creates another raw file nor registers another
payload; the run is still audited as a duplicate. Staging upserts by hashed vehicle
identifier and fuel sequence prevent duplicated warehouse rows.

`staging.vehicles` contains typed vehicle attributes. `staging.fuels` contains
typed fuel attributes. Both use only `vehicle_id_hash` as the join key.

`analytics.ev_vehicles` contains one row per vehicle that has electricity or
hydrogen in its fuel profile. `analytics.ev_fuel_details` retains all fuel types
for those vehicles, which makes hybrid vehicles visible. `analytics.ev_metrics`
aggregates vehicle count, average reported combined CO2, and average net maximum
power by fuel type, brand, model, and registration year. Averages remain `NULL`
when RDW does not provide the underlying values.

## Privacy decision

Licence plates are needed temporarily to make the API join. Exact source responses
therefore contain personal-data-adjacent identifiers and are stored only under the
Git-ignored local raw zone. Before staging, every plate is replaced with a salted
SHA-256 digest. No plaintext licence-plate field exists in staging, analytical, or
Parquet outputs. The salt is kept outside the data products so hashes are not
directly reproducible without local secret state.

Raw files and the salt should not be published. For a production system, access
controls, retention limits, key management, and a formal privacy assessment would
still be required.

## Querying the analytical layer

Open the database with Python:

```python
import duckdb

con = duckdb.connect("data/warehouse/dutch_ev.duckdb", read_only=True)
print(con.sql("""
    SELECT brand, count(*) AS vehicle_count
    FROM analytics.ev_vehicles
    GROUP BY brand
    ORDER BY vehicle_count DESC
    LIMIT 10
""").df())
```

The full set of example queries is in `sql/analytics_examples.sql`. For example:

```sql
SELECT
    fuel_type,
    brand,
    model,
    registration_year,
    vehicle_count,
    avg_co2_combined_g_km,
    avg_net_max_power_kw
FROM analytics.ev_metrics
ORDER BY vehicle_count DESC, brand, model
LIMIT 50;
```

## Testing and operational behaviour

Run `.\.venv\Scripts\python.exe -m pytest`. Tests mock the external API and cover
request construction and errors, date and numeric normalization, stable hashing,
end-to-end model creation, Parquet output, privacy rules, quality checks, and
duplicate ingestion.

Every log line is JSON and includes consistent fields such as timestamp, level,
event, ingestion ID, dataset, row count, and output path where applicable.
Transient HTTP failures use bounded exponential retries. A failed pipeline records
its error in `meta.ingestion_runs` before returning a non-zero process result.
