select distinct
    v.ingestion_id,
    r.started_at as snapshot_started_at,
    r.ingestion_mode,
    r.requested_limit,
    r.page_size,
    r.api_requests,
    r.source_rows_received,
    r.resumed,
    r.resume_count
from {{ ref('stg_vehicles') }} as v
left join {{ ref('stg_ingestion_runs') }} as r
    on v.ingestion_id = r.ingestion_id
