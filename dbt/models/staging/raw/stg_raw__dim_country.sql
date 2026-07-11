with source as (
    select * from {{ source('raw', 'dim_country') }}
)

select
    country_key,
    country_code,
    country_name,
    gold_loaded_at
from source
