{{
  config(
    materialized='table'
  )
}}

with daily_data as (
    select
        symbol,
        date_trunc('day', timestamp) as date,
        count(*) as tick_count,
        avg(price) as avg_price,
        min(price) as low_price,
        max(price) as high_price,
        first_value(price) over (partition by symbol, date_trunc('day', timestamp) order by timestamp) as open_price,
        last_value(price) over (partition by symbol, date_trunc('day', timestamp) order by timestamp) as close_price,
        sum(volume) as total_volume,
        avg(volume) as avg_volume,
        -- Calculate price change
        last_value(price) over (partition by symbol, date_trunc('day', timestamp) order by timestamp) - 
        first_value(price) over (partition by symbol, date_trunc('day', timestamp) order by timestamp) as price_change,
        -- Calculate price change percentage
        case 
            when first_value(price) over (partition by symbol, date_trunc('day', timestamp) order by timestamp) > 0
            then ((last_value(price) over (partition by symbol, date_trunc('day', timestamp) order by timestamp) - 
                   first_value(price) over (partition by symbol, date_trunc('day', timestamp) order by timestamp)) / 
                   first_value(price) over (partition by symbol, date_trunc('day', timestamp) order by timestamp)) * 100
            else null
        end as price_change_percentage
    from {{ ref('stg_tick_data') }}
    group by symbol, date_trunc('day', timestamp)
),

final as (
    select
        symbol,
        date,
        tick_count,
        avg_price,
        low_price,
        high_price,
        open_price,
        close_price,
        total_volume,
        avg_volume,
        price_change,
        price_change_percentage,
        -- Calculate volatility (standard deviation)
        stddev(avg_price) over (partition by symbol order by date rows between 6 preceding and current row) as volatility_7d,
        -- Calculate moving averages
        avg(avg_price) over (partition by symbol order by date rows between 6 preceding and current row) as ma_7d,
        avg(avg_price) over (partition by symbol order by date rows between 29 preceding and current row) as ma_30d
    from daily_data
)

select * from final
