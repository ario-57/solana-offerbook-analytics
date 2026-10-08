select *
from {{ ref('mart_offerbook_daily_execution') }}
where coalesce(successful_transactions, 0)
    + coalesce(failed_transactions, 0)
    != coalesce(transactions, 0)
