with source as (
    select * from {{ source('crypto', 'crypto_ticks') }}
)

select
    id,
    symbol,
    price,
    quantity,
    to_timestamp(trade_time / 1000) as trade_time,
    ingested_at
from source
