with orders as (
    select
        *,
        to_char(order_timestamp, 'YYYY-MM') as year_month
    from {{ ref('stg_raw__fct_orders') }}
),

monthly_orders as (
    select
        year_month,
        sum(gross_revenue) as gross_revenue,
        sum(net_revenue) as net_revenue,
        count(order_key) as order_count,
        count(distinct customer_key) as monthly_active_customers
    from orders
    group by 1
),

first_order as (
    select * from {{ ref('int_customer_first_order') }}
),

orders_with_first as (
    select
        orders.*,
        first_order.first_order_timestamp
    from orders
    left join first_order
        on orders.customer_key = first_order.customer_key
),

new_returning as (
    select
        year_month,
        count(
            distinct case
                when to_char(first_order_timestamp, 'YYYY-MM') = year_month
                    then customer_key
            end
        ) as new_customers,
        count(
            distinct case
                when to_char(first_order_timestamp, 'YYYY-MM') != year_month
                    then customer_key
            end
        ) as returning_customers
    from orders_with_first
    group by 1
)

select
    monthly_orders.year_month,
    monthly_orders.gross_revenue,
    monthly_orders.net_revenue,
    monthly_orders.order_count,
    monthly_orders.monthly_active_customers,
    coalesce(new_returning.new_customers, 0) as new_customers,
    coalesce(new_returning.returning_customers, 0) as returning_customers,
    {{ safe_divide('monthly_orders.net_revenue', 'monthly_orders.order_count') }}
        as average_order_value,
    current_timestamp() as dbt_loaded_at
from monthly_orders
left join new_returning
    on monthly_orders.year_month = new_returning.year_month
