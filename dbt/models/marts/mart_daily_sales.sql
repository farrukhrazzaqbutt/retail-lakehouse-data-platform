with orders_daily as (
    select * from {{ ref('int_orders_daily') }}
),

payments_daily as (
    select * from {{ ref('int_payments_daily') }}
),

joined as (
    select
        orders_daily.sale_date,
        orders_daily.gross_revenue,
        orders_daily.net_revenue,
        orders_daily.order_count,
        orders_daily.cancelled_order_count,
        orders_daily.refunded_order_count,
        coalesce(payments_daily.payment_failure_count, 0) as payment_failure_count,
        coalesce(payments_daily.payment_attempt_count, 0) as payment_attempt_count
    from orders_daily
    left join payments_daily
        on orders_daily.sale_date = payments_daily.sale_date
)

select
    sale_date,
    gross_revenue,
    net_revenue,
    order_count,
    cancelled_order_count,
    refunded_order_count,
    payment_failure_count,
    payment_attempt_count,
    {{ safe_divide('net_revenue', 'order_count') }} as average_order_value,
    {{ safe_divide('payment_failure_count', 'payment_attempt_count') }}
        as payment_failure_rate,
    {{ safe_divide('cancelled_order_count', 'order_count') }}
        as cancellation_rate,
    current_timestamp() as dbt_loaded_at
from joined
