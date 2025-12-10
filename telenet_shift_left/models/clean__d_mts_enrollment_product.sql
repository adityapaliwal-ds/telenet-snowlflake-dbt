with source as (
    select 
        *
    from {{ source('product_offering_explorer', 'd_mts_enrollment_product') }}
)

, active_products as (
    select
        *
    from source
    where star_delete_time is null
)

, product_mapping as (
    select 
        pp.*,
        case
            -- ----------------------------------------------------------------
            -- COMBO PRODUCTS
            -- ----------------------------------------------------------------
            when pp.product_desc in (
                'Combo Netflix & Streamz', 'Combo Netflix & Streamz B2B'
            ) then 'Combo Netflix & Streamz'

            when pp.product_desc in (
                'Combo Netflix & Streamz+', 'Combo Netflix & Streamz+ B2B'
            ) then 'Combo Netflix & Streamz+'

            when pp.product_desc in (
                'Combo Netflix & Streamz Premium', 'Combo Netflix & Streamz Premium B2B'
            ) then 'Combo Netflix & Streamz Premium'

            when pp.product_desc in (
                'Combo Netflix & Streamz Premium+', 'Combo Netflix & Streamz Premium+ B2B'
            ) then 'Combo Netflix & Streamz Premium+'

            when pp.product_desc in (
                'Combo Netflix & Play More', 'Combo Netflix & Play More B2B'
            ) then 'Combo Netflix & Play More'

            -- --------------------------------------------------------
            -- STREAMZ FAMILY  (groupable under "Streamz tiers")
            -- Prior ORIGIN migration names + post migration names
            -- --------------------------------------------------------
            -- prior ORIGIN migration
            when
                pp.product_desc in ('Streamz', 'Streamz B2B')
                and star_date_id < 20240101
                then 'Streamz Premium'

            when
                pp.product_desc in ('Streamz+', 'Streamz+ B2B')
                and star_date_id < 20240101
                then 'Streamz Premium+'

            when
                pp.product_desc in ('Play More', 'Play More B2B')
                and star_date_id < 20240101
                then 'Play More'

            -- post ORIGIN migration
            when
                pp.product_desc in ('Streamz Basic', 'Streamz Basic B2B')
                and star_date_id >= 20240101
                then 'Streamz Basic'

            when
                pp.product_desc in ('Streamz Premium', 'Streamz Premium B2B')
                and star_date_id >= 20240101
                then 'Streamz Premium'

            when
                pp.product_desc in ('Streamz Premium+', 'Streamz Premium+ B2B')
                and star_date_id >= 20240101
                then 'Streamz Premium+'

            ----------------------------------------------------------
            -- DISNEY+ FAMILY (groupable under "Disney+ tiers")
            -- Use star_date_id to handle rename in Oct 2024
            ----------------------------------------------------------
            -- Before Oct 2024 the premium tier appears as "Disney+"
            when
                pp.product_desc in ('Disney+', 'Disney+ B2B')
                and star_date_id < 20241001
                then 'Disney+ Premium'

            -- After rename, premium appears as "Disney+ Premium"
            when
                pp.product_desc in ('Disney+ Premium', 'Disney+ Premium B2B')
                and star_date_id >= 20241001
                then 'Disney+ Premium'

            -- Disney+ Standard – new tier from Sept 2025
            when pp.product_desc in ('Disney+ Standard', 'Disney+ Standard B2B')
                then 'Disney+ Standard'

            ----------------------------------------------------------
            -- NETFLIX FAMILY
            ----------------------------------------------------------
            when pp.product_desc in ('Netflix Basic', 'Netflix Basic B2B')
                then 'Netflix Basic'

            when pp.product_desc in ('Netflix Standard', 'Netflix Standard B2B')
                then 'Netflix Standard'

            when pp.product_desc in ('Netflix Premium', 'Netflix Premium B2B')
                then 'Netflix Premium'
            else pp.product_desc
        end as cleaned_entertainment_product
    from active_products pp
)

select * from product_mapping