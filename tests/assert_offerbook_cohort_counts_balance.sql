select
    cohort_date,
    principal_mint,
    collateral_mint,

    originated_loans,
    active_loans,
    repaid_loans,
    defaulted_loans

from {{ ref('mart_offerbook_loan_cohorts') }}

where originated_loans
      !=
      (
          active_loans
          + repaid_loans
          + defaulted_loans
      )