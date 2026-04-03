with ticks as (
    select * from {{ ref('stg_crypto_ticks') }}
)

select
    symbol,
    date_trunc('minute', trade_time) as minute,
    avg(price) as avg_price,
    stddev(price) as price_stddev,
    max(price) - min(price) as price_range,
    count(*) as tick_count
from ticks
group by symbol, date_trunc('minute', trade_time)
