select
    cohort_date,
    principal_asset_key,
    collateral_asset_key,
    count(*) as row_count

from {{ ref('mart_offerbook_loan_cohorts') }}

group by
    1, 2, 3

having count(*) > 1