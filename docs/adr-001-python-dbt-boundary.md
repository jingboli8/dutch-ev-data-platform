# ADR 001: Python owns ingestion; dbt owns analytics

- Status: Accepted
- Date: 2026-07-28

## Context

The Phase 1 pipeline already provides privacy-aware RDW extraction, keyset
pagination, content-addressed raw storage, transactional staging upserts,
checkpoint recovery, and operational metadata. Reimplementing those behaviours
in dbt would weaken recovery guarantees and force an SQL transformation tool to
own API and file-system concerns.

At the same time, analytical SQL embedded in Python is harder to document, test
at the relation level, and expose as a clear lineage graph.

## Decision

Python remains authoritative for extraction and staging:

- RDW HTTP requests, retry, pagination, and resume;
- raw response persistence and SHA-256 auditing;
- salted hashing before warehouse ingestion;
- DuckDB staging tables and ingestion metadata;
- orchestration, checkpoint state, and Parquet publication.

dbt becomes authoritative above the staging boundary:

- typed staging views;
- reusable intermediate fuel-profile logic;
- dimensions, facts, and analytical marts;
- model documentation, lineage, and SQL data-quality tests.

Python closes its DuckDB connection before starting dbt as a separate process.
The subprocess receives arguments as a list and uses the repository-local
project and profile, avoiding shell quoting and global profile state.

## Recovery consequence

Once staging checks pass, Python durably records `staging_complete`. If dbt
fails, the checkpoint becomes `transformation_failed`. A subsequent `--resume`
validates the original private checkpoint and raw anchor, skips RDW extraction,
and rebuilds dbt models from the already committed staging snapshot.

The first Phase 2 build removes only the explicitly known Phase 1 analytical
relations. It does not drop arbitrary user tables. Repeated dbt builds replace
the dbt-owned relations idempotently.

## Consequences

- There is one authoritative implementation of analytical SQL.
- dbt can be tested in CI with a synthetic DuckDB staging fixture and no network.
- DuckDB file locking is explicit and process boundaries are easy to reason about.
- Final Parquet files are published only after a successful dbt build.
- The platform still represents a current resumable snapshot, not source CDC or
  a transactionally frozen historical series.
