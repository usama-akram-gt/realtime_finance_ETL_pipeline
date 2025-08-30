{{
  config(
    materialized='view'
  )
}}

with source as (
    select * from {{ source('raw', 'tick_data') }}
),

cleaned as (
    select
        id,
        symbol,
        timestamp,
        price,
        volume,
        bid,
        ask,
        bid_size,
        ask_size,
        source,
        exchange,
        created_at,
        -- Data quality checks
        case 
            when price > 0 then true 
            else false 
        end as is_valid_price,
        case 
            when volume >= 0 then true 
            else false 
        end as is_valid_volume,
        case 
            when timestamp is not null then true 
            else false 
        end as is_valid_timestamp
    from source
),

final as (
    select
        id,
        symbol,
        timestamp,
        price,
        volume,
        bid,
        ask,
        bid_size,
        ask_size,
        source,
        exchange,
        created_at,
        is_valid_price,
        is_valid_volume,
        is_valid_timestamp,
        -- Calculate spread if bid and ask are available
        case 
            when bid is not null and ask is not null and ask > bid 
            then ask - bid 
            else null 
        end as spread,
        -- Calculate spread percentage
        case 
            when bid is not null and ask is not null and ask > bid 
            then ((ask - bid) / bid) * 100 
            else null 
        end as spread_percentage
    from cleaned
)

select * from final
