{{
  config(
    materialized='table'
  )
}}

with symbol_data as (
    select distinct
        symbol,
        exchange,
        source,
        min(timestamp) as first_seen,
        max(timestamp) as last_seen,
        count(*) as total_records,
        avg(price) as avg_price,
        min(price) as min_price,
        max(price) as max_price,
        sum(volume) as total_volume
    from {{ ref('stg_tick_data') }}
    group by symbol, exchange, source
),

final as (
    select
        symbol,
        exchange,
        source,
        first_seen,
        last_seen,
        total_records,
        avg_price,
        min_price,
        max_price,
        total_volume,
        -- Calculate price volatility
        (max_price - min_price) / avg_price as price_volatility,
        -- Calculate days active
        date_part('day', last_seen - first_seen) as days_active,
        -- Current status
        case 
            when last_seen >= current_timestamp - interval '1 day' 
            then 'active' 
            else 'inactive' 
        end as status
    from symbol_data
)

select * from final
