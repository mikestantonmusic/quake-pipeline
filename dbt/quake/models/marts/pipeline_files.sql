-- One row per file per COPY attempt, for the dashboard's pipeline health page.
-- Lives in marts so the dashboard's role never needs access to the raw schema.

with audit as (
    select
        source_file,
        status,
        errors_seen,
        first_error,
        loaded_at
    from {{ source('usgs', 'load_audit') }}
),

raw_files as (
    select
        source_file,
        file_last_modified,
        loaded_at,
        array_size(payload:features) as n_features
    from {{ source('usgs', 'feed_files') }}
)

select
    audit.source_file,
    split_part(audit.source_file, '/', 2) as source,
    audit.status,
    raw_files.n_features,
    audit.errors_seen,
    audit.first_error,
    raw_files.file_last_modified,
    audit.loaded_at
from audit
-- A file rewritten in S3 is loaded again, so match each audit row to the copy of the file
-- loaded by that same COPY: the newest one loaded before the audit row was written.
left join raw_files
    on
        audit.source_file = raw_files.source_file
        and audit.status = 'LOADED'
        and audit.loaded_at >= raw_files.loaded_at
qualify row_number() over (
    partition by audit.source_file, audit.loaded_at
    order by raw_files.loaded_at desc nulls last
) = 1
