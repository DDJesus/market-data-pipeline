with observations as (

    select *
    from {{ ref('stg_vendor_b__observations') }}

),

company_identity as (

    select *
    from {{ ref('company_identity') }}

),

resolved as (

    select
        observations.observation_id,
        observations.batch_id,
        observations.observed_at,
        observations.company,
        company_identity.ticker,
        observations.channel,
        observations.sentiment,
        observations.engagement
    from observations
    left join company_identity
        on observations.company = company_identity.company

)

select *
from resolved