{{ config(
    materialized='view'
) }}

with accounts as (

    select
        signature,
        instruction_index,
        instruction_name,
        account_role,
        account_address

    from {{ ref('int_offerbook_instruction_accounts') }}

    where instruction_name in (
        'fill_token_principal_offer',
        'fill_token_collateral_offer'
    )

),

pivoted as (

    select
        signature,
        instruction_index,

        max(
            case
                when account_role = 'signer'
                then account_address
            end
        ) as signer_address,

        max(
            case
                when account_role = 'signer_user'
                then account_address
            end
        ) as signer_user_address,

        max(
            case
                when account_role = 'lender'
                then account_address
            end
        ) as lender_address,

        max(
            case
                when account_role = 'borrower'
                then account_address
            end
        ) as borrower_address,

        max(
            case
                when account_role = 'offer'
                then account_address
            end
        ) as offer_address,

        max(
            case
                when account_role = 'loan'
                then account_address
            end
        ) as loan_address,

        max(
            case
                when account_role = 'principal_mint'
                then account_address
            end
        ) as principal_mint,

        max(
            case
                when account_role = 'collateral_mint'
                then account_address
            end
        ) as collateral_mint

    from accounts

    group by
        signature,
        instruction_index

)

select *
from pivoted