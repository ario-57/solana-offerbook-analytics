{{ config(
    materialized='view'
) }}

with offer_events as (

    select
        signature,
        parent_instruction_index,
        inner_instruction_index,
        block_timestamp,
        event_name,
        decoded_event,
        decode_status,
        idl_version,
        idl_sha256

    from {{ ref('stg_offerbook_events') }}

    where event_name in (
        'OfferCreated',
        'OfferCreatedV1',
        'OfferCreatedV2',
        'OfferFilled',
        'OfferFilledV1',
        'OfferFilledV2',
        'OfferCancelled',
        'OfferCancelledV1',
        'OfferCancelledV2'
    )
      and is_success = true
      and decode_status = 'ok'
),

parsed as (

    select
        signature,
        parent_instruction_index,
        inner_instruction_index,
        block_timestamp,

        event_name,

        case
            when event_name like 'OfferCreated%' then 'created'
            when event_name like 'OfferFilled%' then 'filled'
            when event_name like 'OfferCancelled%' then 'cancelled'
        end as event_type,

        case
            when event_name in ('OfferCreated', 'OfferFilled', 'OfferCancelled')
                then 'v0'
            when event_name in ('OfferCreatedV1', 'OfferFilledV1', 'OfferCancelledV1')
                then 'v1'
            when event_name in ('OfferCreatedV2', 'OfferFilledV2', 'OfferCancelledV2')
                then 'v2'
        end as event_version,

        json_extract_string(decoded_event, '$.pubkey')
            as offer_address,

        json_extract_string(decoded_event, '$.offer.creator')
            as creator_address,

        json_extract_string(decoded_event, '$.offer.side.variant')
            as offer_side,

        json_extract_string(decoded_event, '$.offer.status.variant')
            as offer_status,

        json_extract_string(decoded_event, '$.offer.principal.variant')
            as principal_asset_type,

        json_extract(decoded_event, '$.offer.principal')
            as principal_asset,

        json_extract_string(decoded_event, '$.offer.collateral.variant')
            as collateral_asset_type,

        json_extract(decoded_event, '$.offer.collateral')
            as collateral_asset,

        try_cast(
            json_extract_string(decoded_event, '$.offer.principal_amount')
            as ubigint
        ) as principal_amount_raw,

        try_cast(
            json_extract_string(decoded_event, '$.offer.remaining_principal')
            as ubigint
        ) as remaining_principal_raw,

        try_cast(
            json_extract_string(decoded_event, '$.offer.collateral_amount')
            as ubigint
        ) as collateral_amount_raw,

        try_cast(
            json_extract_string(decoded_event, '$.offer.remaining_collateral')
            as ubigint
        ) as remaining_collateral_raw,

        try_cast(
            json_extract_string(decoded_event, '$.offer.apy')
            as bigint
        ) as apy_raw,

        try_cast(
            json_extract_string(decoded_event, '$.offer.duration')
            as bigint
        ) as duration_raw,

        try_cast(
            json_extract_string(decoded_event, '$.offer.created_at')
            as bigint
        ) as created_at_raw,

        try_cast(
            json_extract_string(decoded_event, '$.offer.expired_at')
            as bigint
        ) as expired_at_raw,

        try_cast(
            json_extract_string(decoded_event, '$.offer.updated_at')
            as bigint
        ) as updated_at_raw,

        try_cast(
            json_extract_string(decoded_event, '$.offer.min_fill_amount')
            as ubigint
        ) as min_fill_amount_raw,

        try_cast(
            json_extract_string(decoded_event, '$.offer.fill_counter')
            as ubigint
        ) as fill_counter,

        try_cast(
            json_extract_string(decoded_event, '$.offer.allow_partial_fill')
            as integer
        ) = 1 as allow_partial_fill,

        case
            when event_name like '%V2'
            then
                try_cast(
                    json_extract_string(decoded_event, '$.offer.allow_extend')
                    as integer
                ) = 1
        end as allow_extend,

        case
            when event_name like '%V1'
              or event_name like '%V2'
            then json_extract_string(
                decoded_event,
                '$.offer.countered_offer'
            )
        end as countered_offer_address,

        idl_version,
        idl_sha256

    from offer_events
),

final as (

    select
        *,

        to_timestamp(created_at_raw)
            as offer_created_at,

        to_timestamp(expired_at_raw)
            as offer_expired_at,

        to_timestamp(updated_at_raw)
            as offer_updated_at,

        {{ offerbook_asset_key(
            'principal_asset',
            'principal_asset_type'
        ) }} as principal_asset_key,

        {{ offerbook_asset_key(
            'collateral_asset',
            'collateral_asset_type'
        ) }} as collateral_asset_key

    from parsed
)

select *
from final
