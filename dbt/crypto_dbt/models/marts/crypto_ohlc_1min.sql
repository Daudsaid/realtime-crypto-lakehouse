with ticks as (
    select * from {{ ref('stg_crypto_ticks') }}
),

ohlc as (
    select
        symbol,
        date_trunc('minute', trade_time) as minute,
        min(price) as low,
        max(price) as high,
        sum(quantity) as volume,
        min(trade_time) as first_trade_time,
        max(trade_time) as last_trade_time
    from ticks
    group by symbol, date_trunc('minute', trade_time)
),

open_prices as (
    select distinct on (symbol, date_trunc('minute', trade_time))
        symbol,
        date_trunc('minute', trade_time) as minute,
        price as open
    from ticks
    order by symbol, date_trunc('minute', trade_time), trade_time asc
),

close_prices as (
    select distinct on (symbol, date_trunc('minute', trade_time))
        symbol,
        date_trunc('minute', trade_time) as minute,
        price as close
    from ticks
    order by symbol, date_trunc('minute', trade_time), trade_time desc
)

select
    o.symbol,
    o.minute,
    op.open,
    o.high,
    o.low,
    cp.close,
    o.volume
from ohlc o
join open_prices op on op.symbol = o.symbol and op.minute = o.minute
join close_prices cp on cp.symbol = o.symbol and cp.minute = o.minute
