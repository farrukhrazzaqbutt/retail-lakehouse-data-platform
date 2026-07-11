select
    items.order_item_key,
    items.order_key,
    items.product_key,
    items.quantity,
    items.unit_price,
    items.discount_pct,
    items.line_total,
    orders.is_completed,
    orders.gross_revenue as order_gross_revenue,
    orders.net_revenue as order_net_revenue,
    products.product_id,
    products.category,
    products.subcategory,
    products.product_name,
    items.gold_loaded_at
from {{ ref('stg_raw__fct_order_items') }} as items
inner join {{ ref('stg_raw__fct_orders') }} as orders
    on items.order_key = orders.order_key
inner join {{ ref('stg_raw__dim_products') }} as products
    on items.product_key = products.product_key
