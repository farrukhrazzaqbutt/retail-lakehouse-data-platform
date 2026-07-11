select
    orders.order_key,
    orders.customer_key,
    orders.order_timestamp,
    orders.order_status,
    orders.is_completed,
    orders.is_cancelled,
    orders.is_refunded,
    orders.gross_revenue,
    orders.net_revenue,
    customers.customer_segment,
    customers.country_code,
    customers.country_name
from {{ ref('stg_raw__fct_orders') }} as orders
inner join {{ ref('stg_raw__dim_customers') }} as customers
    on orders.customer_key = customers.customer_key
