with source as (

    select *
    from {{ source('vendor_b', 'vendor_b_observation') }}

),

renamed as (

    select
        observation_id,
        batch_id,
        schema_version,
        observed_at,
        company,
        channel,
        sentiment,
        engagement
    from source

)

select *
from renamed