select
    loan_address,
    principal_mint

from {{ ref('int_offerbook_loans_enriched') }}

where principal_asset_type = 'Token'
  and principal_decimals is null