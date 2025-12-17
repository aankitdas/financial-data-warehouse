{{ config(
    materialized='view',
    tags=['staging']
) }}

select
    "ticker" as ticker,
    "date"::date as price_date,
    "open_price" as open_price,
    "close_price" as close_price,
    "high_price" as high_price,
    "low_price" as low_price,
    "volume" as volume,
    "ingested_at" as ingested_at,
    round(("close_price" - "open_price") / "open_price" * 100, 2) as daily_change_pct,
    round("high_price" - "low_price", 2) as daily_range
from "FINANCE_WAREHOUSE"."RAW"."STG_STOCK_PRICES"
where "date" is not null
  and "close_price" > 0
qualify row_number() over (partition by "ticker", "date" order by "ingested_at" desc) = 1