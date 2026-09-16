{% macro offerbook_asset_key(asset_json, asset_type) %}

    case

        when {{ asset_type }} is null
            then null

        --------------------------------------------------
        -- Token / mint-based NFT
        --------------------------------------------------

        when json_extract_string(
            {{ asset_json }},
            '$.fields[0].mint'
        ) is not null

        then concat(
            {{ asset_type }},
            ':',
            json_extract_string(
                {{ asset_json }},
                '$.fields[0].mint'
            )
        )

        --------------------------------------------------
        -- Core NFT
        --------------------------------------------------

        when json_extract_string(
            {{ asset_json }},
            '$.fields[0].asset'
        ) is not null

        then concat(
            {{ asset_type }},
            ':',
            json_extract_string(
                {{ asset_json }},
                '$.fields[0].asset'
            )
        )

        --------------------------------------------------
        -- Defensive fallback
        --
        -- If Offerbook adds another EventAsset variant,
        -- we still get a stable non-null representation
        -- instead of silently collapsing assets together.
        --------------------------------------------------

        else concat(
            {{ asset_type }},
            ':',
            cast(
                {{ asset_json }}
                as varchar
            )
        )

    end

{% endmacro %}