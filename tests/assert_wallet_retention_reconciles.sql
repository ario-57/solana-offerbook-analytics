select *
from {{ ref('mart_offerbook_daily_wallet_retention') }}
where coalesce(new_wallets, 0)
    + coalesce(returning_wallets, 0)
    != coalesce(active_wallets, 0)
