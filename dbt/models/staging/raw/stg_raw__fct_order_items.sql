with source as (
    select * from {{ source('raw', 'fct_order_items') }}
)

select
    order_item_key,
    order_key,
    product_key,
    quantity,
    unit_price,
    discount_pct,
    line_total,
    gold_loaded_at
from source
