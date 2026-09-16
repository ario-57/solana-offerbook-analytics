select
    *

from {{ ref('mart_offerbook_lifecycle_overview') }}

where

    total_loans
    !=
    (
        active_loans
        + repaid_loans
        + defaulted_loans
    )

    or

    closed_loans
    !=
    (
        repaid_loans
        + defaulted_loans
    )