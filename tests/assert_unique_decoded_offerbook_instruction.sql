select
    signature,
    instruction_index,
    count(*) as row_count

from {{ ref('stg_offerbook_decoded_instructions') }}

group by
    signature,
    instruction_index

having count(*) > 1