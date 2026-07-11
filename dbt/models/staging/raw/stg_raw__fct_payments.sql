with source as (
    select * from {{ source('raw', 'fct_payments') }}
)

select
    payment_key,
    order_key,
    payment_date_key,
    payment_timestamp,
    payment_method,
    payment_status,
    amount,
    is_successful,
    is_failed,
    gold_loaded_at
from source
