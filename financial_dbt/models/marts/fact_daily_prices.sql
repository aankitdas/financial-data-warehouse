{{ config(
    materialized='table',
    tags=['marts', 'fact']
) }}

with companies as (
    select company_id, ticker, name, sector
    from {{ ref('stg_companies') }}
),

prices as (
    select
        ticker,
        price_date,
        open_price,
        close_price,
        high_price,
        low_price,
        volume,
        daily_change_pct,
        daily_range
    from {{ ref('stg_stock_prices') }}
),

joined as (
    select
        companies.company_id,
        prices.price_date,
        prices.open_price,
        prices.close_price,
        prices.high_price,
        prices.low_price,
        prices.volume,
        prices.daily_change_pct,
        prices.daily_range,
        current_timestamp() as dbt_load_timestamp
    from prices
    left join companies on prices.ticker = companies.ticker
)

select * from joined