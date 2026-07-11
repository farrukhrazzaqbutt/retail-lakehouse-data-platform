with source as (
    select * from {{ source('raw', 'dim_products') }}
)

select
    product_key,
    product_id,
    sku,
    product_name,
    category,
    subcategory,
    brand,
    unit_price,
    unit_cost,
    is_active,
    gold_loaded_at
from source
