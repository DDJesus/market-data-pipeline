with sentiment as (

    select *
    from {{ ref('int_vendor_b__sentiment_with_identity') }}

),

batches as (

    select *
    from {{ ref('stg_vendor_b__batches') }}

),

enriched as (

    select
        batches.market_date,
        batches.batch_type,
        sentiment.company,
        sentiment.ticker,
        sentiment.sentiment,
        sentiment.engagement
    from sentiment
    inner join batches
        on sentiment.batch_id = batches.batch_id

),

aggregated as (

    select
        market_date,
        batch_type,
        company,
        ticker,

        count(*) as observation_count,

        count(*) filter (
            where sentiment = 'POSITIVE'
        ) as positive_observation_count,

        count(*) filter (
            where sentiment = 'NEUTRAL'
        ) as neutral_observation_count,

        count(*) filter (
            where sentiment = 'NEGATIVE'
        ) as negative_observation_count,

        sum(engagement) as total_engagement,

        avg(engagement) as average_engagement

    from enriched

    group by
        market_date,
        batch_type,
        company,
        ticker

)

select *
from aggregated