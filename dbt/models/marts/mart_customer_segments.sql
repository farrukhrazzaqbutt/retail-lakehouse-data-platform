with customer_orders as (
    select * from {{ ref('int_customer_segment_orders') }}
),

segment_metrics as (
    select
        customer_segment,
        country_code,
        country_name,
        count(distinct customer_key) as customer_count,
        sum(gross_revenue) as total_revenue,
        sum(net_revenue) as net_revenue,
        count(order_key) as order_count,
        sum(case when is_cancelled then 1 else 0 end) as cancelled_orders,
        count(distinct customer_key) as ordering_customers
    from customer_orders
    group by 1, 2, 3
),

repeat_customers as (
    select
        customer_segment,
        country_code,
        sum(case when customer_order_count > 1 then 1 else 0 end)
            as repeat_customers,
        count(*) as customers_with_orders
    from (
        select
            customer_segment,
            country_code,
            customer_key,
            count(order_key) as customer_order_count
        from customer_orders
        group by 1, 2, 3
    ) as customer_order_counts
    group by 1, 2
)

select
    segment_metrics.customer_segment,
    segment_metrics.country_code,
    segment_metrics.country_name,
    segment_metrics.customer_count,
    segment_metrics.total_revenue,
    segment_metrics.net_revenue,
    segment_metrics.order_count,
    segment_metrics.cancelled_orders,
    segment_metrics.ordering_customers,
    coalesce(repeat_customers.repeat_customers, 0) as repeat_customers,
    coalesce(repeat_customers.customers_with_orders, 0) as customers_with_orders,
    {{ safe_divide('segment_metrics.net_revenue', 'segment_metrics.order_count') }}
        as avg_order_value,
    {{ safe_divide(
        'coalesce(repeat_customers.repeat_customers, 0)',
        'coalesce(repeat_customers.customers_with_orders, 0)'
    ) }} as repeat_purchase_rate,
    {{ safe_divide(
        'segment_metrics.cancelled_orders',
        'segment_metrics.order_count'
    ) }} as cancellation_rate,
    current_timestamp() as dbt_loaded_at
from segment_metrics
left join repeat_customers
    on segment_metrics.customer_segment = repeat_customers.customer_segment
    and segment_metrics.country_code = repeat_customers.country_code
