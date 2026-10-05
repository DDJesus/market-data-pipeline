with source as (

    select *
    from {{ source('vendor_b', 'vendor_b_batch') }}

),

renamed as (

    select
        batch_id,
        schema_version,
        market_date,
        batch_type,
        window_start,
        window_end,
        observation_count,
        source_url,
        retrieved_at_utc,
        sha256
    from source

)

select *
from renamed