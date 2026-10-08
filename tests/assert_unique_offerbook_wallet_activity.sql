select
    block_date,
    wallet_address,
    wallet_role,
    count(*) as row_count
from {{ ref('int_offerbook_wallet_activity') }}
group by 1, 2, 3
having count(*) > 1
