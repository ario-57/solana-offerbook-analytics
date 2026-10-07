select
    mint_address,
    price_date,
    count(*) as row_count

from {{ ref('stg_token_prices_daily') }}

group by
    mint_address,
    price_date

having count(*) > 1
