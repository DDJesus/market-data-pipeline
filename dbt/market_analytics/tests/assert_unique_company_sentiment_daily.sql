select
    market_date,
    batch_type,
    ticker,
    count(*) as row_count

from {{ ref('mart_company_sentiment_daily') }}

group by
    market_date,
    batch_type,
    ticker

having count(*) > 1