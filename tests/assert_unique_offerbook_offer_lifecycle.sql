select
    offer_address,
    count(*) as row_count

from {{ ref('int_offerbook_offer_lifecycle') }}

group by 1

having count(*) != 1
