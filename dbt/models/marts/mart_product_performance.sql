select
    product_id,
    category,
    subcategory,
    product_name,
    sum(quantity) as total_quantity_sold,
    sum(line_total) as gross_revenue,
    sum(
        case when is_completed then line_total else 0 end
    ) as net_revenue,
    count(distinct order_key) as order_count,
    case
        when sum(quantity) > 0 then
            sum(case when is_completed then line_total else 0 end) / sum(quantity)
        else 0
    end as avg_unit_revenue,
    current_timestamp() as dbt_loaded_at
from {{ ref('int_order_items_enriched') }}
group by 1, 2, 3, 4
