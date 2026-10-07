select
    block_date,
    principal_asset_key,
    collateral_asset_key,
    creator_role,
    count(*) as row_count

from {{ ref('mart_offerbook_offer_efficiency_daily') }}

group by
    1,
    2,
    3,
    4

having count(*) != 1
