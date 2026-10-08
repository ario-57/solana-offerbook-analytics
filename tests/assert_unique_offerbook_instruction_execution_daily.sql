select
    block_date,
    instruction_name,
    count(*) as row_count
from {{ ref('mart_offerbook_instruction_execution_daily') }}
group by 1, 2
having count(*) > 1
