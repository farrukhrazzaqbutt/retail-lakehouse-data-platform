select
    orders.order_key,
    orders.customer_key,
    orders.order_date_key,
    orders.order_timestamp,
    orders.order_status,
    orders.shipping_country,
    orders.subtotal_amount,
    orders.discount_amount,
    orders.shipping_amount,
    orders.tax_amount,
    orders.total_amount,
    orders.is_completed,
    orders.is_cancelled,
    orders.is_refunded,
    orders.gross_revenue,
    orders.net_revenue,
    customers.customer_segment,
    customers.country_code,
    customers.country_name,
    customers.city,
    orders.gold_loaded_at
from {{ ref('stg_raw__fct_orders') }} as orders
inner join {{ ref('stg_raw__dim_customers') }} as customers
    on orders.customer_key = customers.customer_key
