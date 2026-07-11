with source as (
    select * from {{ source('raw', 'dim_date') }}
)

select
    date_key,
    full_date,
    year,
    quarter,
    month,
    month_name,
    day,
    day_of_week,
    day_name,
    week_of_year,
    is_weekend,
    gold_loaded_at
from source
