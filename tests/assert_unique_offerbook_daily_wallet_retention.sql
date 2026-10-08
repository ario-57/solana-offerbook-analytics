select
    block_date,
    wallet_role,
    count(*) as row_count
from {{ ref('mart_offerbook_daily_wallet_retention') }}
group by 1, 2
having count(*) > 1
