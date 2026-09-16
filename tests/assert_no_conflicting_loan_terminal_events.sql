select
    loan_address,
    terminal_event_count,
    terminal_event_type_count

from {{ ref('int_offerbook_loan_lifecycle') }}

where has_conflicting_terminal_events = true