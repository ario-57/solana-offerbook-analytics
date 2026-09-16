{{ config(
    materialized='view'
) }}

with instructions as (

    select
        i.signature,
        i.instruction_index,
        i.block_timestamp,
        i.is_success,

        d.instruction_name,
        d.decoded_args,
        d.decode_status

    from {{ ref('int_offerbook_instructions') }} as i

    inner join {{ ref('stg_offerbook_decoded_instructions') }} as d
        on i.signature = d.signature
        and i.instruction_index = d.instruction_index

    where d.instruction_name in (
        'create_token_principal_offer',
        'create_token_collateral_offer'
    )

),

offer_arguments as (

    select
        signature,
        instruction_index,
        block_timestamp,
        is_success,
        instruction_name,

        case
            when instruction_name =
                'create_token_principal_offer'
                then 'principal'

            when instruction_name =
                'create_token_collateral_offer'
                then 'collateral'
        end as offer_side,

        try_cast(
            json_extract_string(
                decoded_args,
                '$.offer_params.principal_amount'
            )
            as ubigint
        ) as principal_amount_raw,

        try_cast(
            json_extract_string(
                decoded_args,
                '$.offer_params.collateral_amount'
            )
            as ubigint
        ) as collateral_amount_raw,

        try_cast(
            json_extract_string(
                decoded_args,
                '$.offer_params.apy'
            )
            as bigint
        ) as apy_raw,

        try_cast(
            json_extract_string(
                decoded_args,
                '$.offer_params.duration'
            )
            as bigint
        ) as duration_raw,

        try_cast(
            json_extract_string(
                decoded_args,
                '$.offer_params.expiry'
            )
            as bigint
        ) as expiry_raw,

        try_cast(
            json_extract_string(
                decoded_args,
                '$.offer_params.allow_partial_fill'
            )
            as boolean
        ) as allow_partial_fill,

        try_cast(
            json_extract_string(
                decoded_args,
                '$.offer_params.allow_extend'
            )
            as boolean
        ) as allow_extend,  

        try_cast(
            json_extract_string(
                decoded_args,
                '$.offer_params.min_fill_amount'
            )
            as ubigint
        ) as min_fill_amount_raw,

        decode_status

    from instructions

),

final as (

    select
        a.signature,
        a.instruction_index,
        a.block_timestamp,
        a.is_success,
        a.instruction_name,
        a.offer_side,

        o.signer_address,
        o.signer_user_address,
        o.offer_address,

        o.principal_mint,
        o.collateral_mint,

        o.countered_offer_address,

        a.principal_amount_raw,
        a.collateral_amount_raw,

        a.apy_raw,
        a.duration_raw,
        a.expiry_raw,

        a.allow_partial_fill,
        a.allow_extend,
        a.min_fill_amount_raw,

        a.decode_status

    from offer_arguments as a

    left join {{ ref('int_offerbook_offer_accounts') }} as o
        on a.signature = o.signature
        and a.instruction_index = o.instruction_index

)

select *
from final