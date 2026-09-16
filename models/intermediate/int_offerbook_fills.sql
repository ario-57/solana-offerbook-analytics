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
        'fill_token_principal_offer',
        'fill_token_collateral_offer'
    )

),

fill_arguments as (

    select
        signature,
        instruction_index,
        block_timestamp,
        is_success,
        instruction_name,

        case
            when instruction_name =
                'fill_token_principal_offer'
                then 'principal'

            when instruction_name =
                'fill_token_collateral_offer'
                then 'collateral'
        end as offer_side,

        try_cast(
            json_extract_string(
                decoded_args,
                '$.fill_params.principal_fill_amount'
            )
            as ubigint
        ) as principal_fill_amount_raw,

        try_cast(
            json_extract_string(
                decoded_args,
                '$.fill_params.collateral_fill_amount'
            )
            as ubigint
        ) as collateral_fill_amount_raw,

        try_cast(
            json_extract_string(
                decoded_args,
                '$.fill_params.max_collateral'
            )
            as ubigint
        ) as max_collateral_raw,

        try_cast(
            json_extract_string(
                decoded_args,
                '$.fill_params.max_principal'
            )
            as ubigint
        ) as max_principal_raw,

        try_cast(
            json_extract_string(
                decoded_args,
                '$.fill_params.duration'
            )
            as bigint
        ) as duration_raw,

        try_cast(
            json_extract_string(
                decoded_args,
                '$.fill_params.apy'
            )
            as bigint
        ) as apy_raw,

        json_extract(
            decoded_args,
            '$.fill_params.loan_type'
        ) as loan_type,

        decode_status

    from instructions

),

final as (

    select
        f.signature,
        f.instruction_index,
        f.block_timestamp,
        f.is_success,

        f.instruction_name,
        f.offer_side,

        a.signer_address,
        a.signer_user_address,

        a.lender_address,
        a.borrower_address,

        a.offer_address,
        a.loan_address,

        a.principal_mint,
        a.collateral_mint,

        f.principal_fill_amount_raw,
        f.collateral_fill_amount_raw,

        f.max_principal_raw,
        f.max_collateral_raw,

        f.apy_raw,
        f.duration_raw,
        f.loan_type,

        f.decode_status

    from fill_arguments as f

    left join {{ ref('int_offerbook_fill_accounts') }} as a
        on f.signature = a.signature
        and f.instruction_index = a.instruction_index

)

select *
from final