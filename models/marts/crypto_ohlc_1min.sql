with ticks as (
    select * from {{ ref('stg_crypto_ticks') }}
)

select
    symbol,
    date_trunc('minute', trade_time) as minute,
    min(price) as low,
    max(price) as high,
    first_value(price) over (
        partition by symbol, date_trunc('minute', trade_time)
        order by trade_time
    ) as open,
    last_value(price) over (
        partition by symbol, date_trunc('minute', trade_time)
        order by trade_time
        rows between unbounded preceding and unbounded following
    ) as close,
    sum(quantity) as volume
from ticks
group by symbol, date_trunc('minute', trade_time), price, trade_time
