-- Reconcile dbt mart_daily_sales row counts with RAW Gold mart.
select count(*) as mismatch_count
from (
    select sale_date from {{ ref('mart_daily_sales') }}
    except
    select sale_date from {{ source('raw', 'mart_daily_sales') }}
    union all
    select sale_date from {{ source('raw', 'mart_daily_sales') }}
    except
    select sale_date from {{ ref('mart_daily_sales') }}
) as mismatches
having count(*) > 0
