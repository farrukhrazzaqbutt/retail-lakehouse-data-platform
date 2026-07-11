with source as (
    select * from {{ source('raw', 'dim_customers') }}
)

select
    customer_key,
    customer_id,
    email,
    first_name,
    last_name,
    country_code,
    country_name,
    city,
    customer_segment,
    signup_date,
    is_active,
    gold_loaded_at
from source
