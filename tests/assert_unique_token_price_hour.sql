select
    mint_address,
    price_hour,
    count(*) as row_count

from {{ ref('stg_token_prices_hourly') }}

group by
    mint_address,
    price_hour

having count(*) > 1