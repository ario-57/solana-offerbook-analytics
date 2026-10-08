select *
from {{ ref('mart_offerbook_instruction_execution_daily') }}
where coalesce(successful_transactions, 0)
    + coalesce(failed_transactions, 0)
    != coalesce(transaction_count, 0)
