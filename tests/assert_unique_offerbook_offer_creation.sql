select
    signature,
    instruction_index,
    count(*) as row_count

from {{ ref('int_offerbook_offer_creations') }}

group by
    signature,
    instruction_index

having count(*) > 1