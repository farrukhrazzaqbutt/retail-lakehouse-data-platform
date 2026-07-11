select
    customer_key,
    min(order_timestamp) as first_order_timestamp
from {{ ref('stg_raw__fct_orders') }}
group by 1
