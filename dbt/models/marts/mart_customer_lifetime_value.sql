with customer_orders as (
    select
        customer_key,
        count(order_key) as total_orders,
        sum(gross_revenue) as total_revenue,
        sum(net_revenue) as net_revenue,
        min(order_timestamp) as first_order_date,
        max(order_timestamp) as last_order_date
    from {{ ref('stg_raw__fct_orders') }}
    group by 1
)

select
    customer_key,
    total_orders,
    total_revenue,
    net_revenue,
    first_order_date,
    last_order_date,
    {{ safe_divide('net_revenue', 'total_orders') }} as avg_order_value,
    case when total_orders > 1 then 1.0 else 0.0 end as repeat_purchase_rate,
    current_timestamp() as dbt_loaded_at
from customer_orders
