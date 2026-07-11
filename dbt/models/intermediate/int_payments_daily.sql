select
    cast(payment_timestamp as date) as sale_date,
    sum(case when is_failed then 1 else 0 end) as payment_failure_count,
    count(payment_key) as payment_attempt_count
from {{ ref('stg_raw__fct_payments') }}
group by 1
