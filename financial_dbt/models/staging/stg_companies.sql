
{{ config(
    materialized='view',
    tags=['staging']
) }}

select
    row_number() over (order by "ticker") as company_id,
    "ticker" as ticker,
    "name" as name,
    "sector" as sector,
    "created_at" as created_at,
    "updated_at" as updated_at
from "FINANCE_WAREHOUSE"."RAW"."STG_COMPANIES"
where "ticker" is not null
  and "name" is not null