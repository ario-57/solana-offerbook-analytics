select
    block_date,
    instruction_name,
    count(*) as row_count

from {{ ref('mart_offerbook_daily_activity') }}

group by
    block_date,
    instruction_name

having count(*) > 1