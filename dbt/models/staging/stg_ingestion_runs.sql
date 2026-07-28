select
    cast(ingestion_id as varchar) as ingestion_id,
    cast(started_at as timestamptz) as started_at,
    cast(ingestion_mode as varchar) as ingestion_mode,
    cast(requested_limit as bigint) as requested_limit,
    cast(page_size as integer) as page_size,
    cast(pages_requested as integer) as api_requests,
    cast(source_rows_received as bigint) as source_rows_received,
    cast(matched_vehicles as bigint) as matched_vehicles,
    cast(fuel_rows as bigint) as fuel_rows,
    cast(rejected_rows as bigint) as rejected_rows,
    cast(resumed as boolean) as resumed,
    cast(resume_count as integer) as resume_count
from {{ source('pipeline_metadata', 'ingestion_runs') }}
