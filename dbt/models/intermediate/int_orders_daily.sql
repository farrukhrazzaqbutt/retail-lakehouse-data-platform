select
    cast(order_timestamp as date) as sale_date,
    sum(gross_revenue) as gross_revenue,
    sum(net_revenue) as net_revenue,
    count(order_key) as order_count,
    sum(case when is_cancelled then 1 else 0 end) as cancelled_order_count,
    sum(case when is_refunded then 1 else 0 end) as refunded_order_count
from {{ ref('stg_raw__fct_orders') }}
group by 1
