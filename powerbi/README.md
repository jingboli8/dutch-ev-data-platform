# Dutch EV Analytics Power BI Project

This directory contains the Power BI Project (PBIP) report and semantic model
for the Dutch EV Data Platform. The dashboard presents a privacy-safe,
bounded RDW vehicle snapshot produced by the Python and dbt pipeline.

## Report pages

### Snapshot Overview

Provides a concise view of the current snapshot. Its main metrics and visuals
cover:

- vehicles represented in the snapshot;
- battery-electric, hybrid-electric, and hydrogen-electric composition;
- the operational snapshot start time;
- leading vehicle brands; and
- the distribution of RDW first-registration years.

### Manufacturer & Model Mix

Explores the composition of the snapshot by manufacturer, model, vehicle type,
and powertrain category. It includes leading-brand analysis, a
brand/model/type hierarchy, vehicle counts, the share of vehicles with
multiple fuel records, and the distribution of first-registration years.

### Fuel & Technical Profile

Examines the RDW fuel-detail records associated with the vehicles. Its main
metrics include:

- distinct vehicles having the selected reported fuel type;
- fuel-record count;
- average reported combined CO2 in grams per kilometre;
- average reported net maximum power in kilowatts; and
- CO2 and power reporting coverage.

The page also breaks these metrics down by reported fuel type and hybrid
class.

## Prerequisites

- A local checkout of this repository.
- Power BI Desktop with Power BI Project and TMDL support.
- A completed analytical Parquet publication from the repository pipeline.

The semantic model requires these two files in the same local directory:

```text
analytics_mart_ev_overview.parquet
analytics_fact_vehicle_fuel.parquet
```

These filenames are taken directly from the two semantic-model partition
definitions. The generated Parquet files are local data artifacts and must not
be committed.

## Configure the local Parquet directory

The semantic model uses one text Power Query parameter named `ParquetRoot`.
Before refreshing the model locally:

1. Open Power BI Desktop.
2. Use **Transform data > Manage parameters**.
3. Select `ParquetRoot`.
4. Set its current value to the full local path of the directory containing
   the two required Parquet files.
5. Apply the change and refresh the model.

For the standard repository layout, this is the local `data\parquet`
directory under the repository root. Do not commit a machine-specific absolute
path.

`SET_LOCAL_PARQUET_ROOT` is the intended publication placeholder. It is a
deliberately non-working value, not a data directory. A local user must replace
the placeholder in Power BI Desktop before refreshing data. The committed model
uses `SET_LOCAL_PARQUET_ROOT` as a placeholder. Before refreshing, configure
`ParquetRoot` to point to your local Parquet directory.

## Open the project

After configuring the local Parquet directory, open
`powerbi/DutchEVAnalytics.pbip` in Power BI Desktop. The PBIP file links the
report to the repository-local semantic-model project by relative path.

## Semantic model

The report imports two analytical tables:

- **EV Overview**: one row per privacy-safe vehicle within the represented
  ingestion snapshot.
- **Vehicle Fuel**: one row per ingestion snapshot, privacy-safe vehicle, and
  RDW fuel sequence.

The active business relationship is:

```text
EV Overview[vehicle_snapshot_key] 1 --> * Vehicle Fuel[vehicle_snapshot_key]
```

It uses single-direction filtering from **EV Overview** to **Vehicle Fuel**.
Power BI may also maintain hidden automatic date tables for date navigation;
those are separate from the vehicle-to-fuel relationship.

## Metric interpretation

- **Vehicle counts** count distinct or one-row-per-snapshot vehicles.
- **Fuel Records** counts fuel-detail rows. It is not a vehicle count.
- One vehicle can have multiple fuel records and can therefore appear under
  multiple reported fuel types.
- CO2 and power averages use only non-null values reported on fuel-detail
  records. Missing source values are not replaced with zero.
- CO2 and power coverage use fuel records as the denominator. A zero coverage
  value means fuel rows exist but none reports the relevant value; a blank can
  represent a context with no fuel rows.
- An `Elektriciteit` fuel row is not equivalent to a battery-electric vehicle.
  Fuel type and derived powertrain category are different analytical concepts.
- RDW first-registration dates are not vehicle-sale events and must not be
  interpreted as sales.
- The dashboard represents a bounded, resumable RDW snapshot configured by the
  pipeline run. It is not the entire Dutch vehicle fleet and is not a
  historical time series.

## Privacy and repository safety

- Power BI `.pbi` workspace directories, caches, local settings, and security
  bindings are local-only and must remain ignored.
- Raw RDW payloads, generated Parquet files, DuckDB warehouses, logs, and local
  state must not be committed.
- Credentials, API tokens, the real vehicle-hashing salt, and local absolute
  paths must never be stored in tracked Power BI files or documentation.
- The report must not expose plaintext licence plates or other row-level source
  identifiers. Technical privacy-safe keys are hidden from report view.

## Validation status

The following results describe prior checks. They were not rerun while creating
this documentation.

### Prior structural and data checks

- The semantic-model TMDL previously deserialized successfully with the Power
  BI TMDL serializer.
- The model previously reconciled to its local analytical Parquet inputs.
- One validated example snapshot contained 50,000 vehicles and 94,047 fuel
  records. These are example results from that bounded run, not permanent
  dashboard guarantees.

### User-confirmed Power BI Desktop checks

- A filter context containing vehicles but no multi-fuel qualifiers displayed
  numeric zero for the multi-fuel result.
- For reported fuel type `CNG`, Desktop displayed:
  - 24 vehicles;
  - 24 fuel records;
  - blank average reported CO2;
  - 0.0% CO2 reporting coverage;
  - 215.8 kW average reported power; and
  - 100.0% power reporting coverage.

The no-fuel-records filter context has **not** been tested in Power BI Desktop.
That case remains an explicit validation item before publication.
