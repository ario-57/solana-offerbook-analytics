select
    principal_asset_key,
    collateral_asset_key,
    creator_role,
    count(*) as row_count

from {{ ref('mart_offerbook_market_liquidity_snapshot') }}

group by
    1,
    2,
    3

having count(*) != 1
