with source as (
    select * from {{ source('raw', 'fct_orders') }}
)

select
    order_key,
    customer_key,
    order_date_key,
    order_timestamp,
    order_status,
    shipping_country,
    subtotal_amount,
    discount_amount,
    shipping_amount,
    tax_amount,
    total_amount,
    is_completed,
    is_cancelled,
    is_refunded,
    gross_revenue,
    net_revenue,
    gold_loaded_at
from source
