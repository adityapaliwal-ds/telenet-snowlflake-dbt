with base as (
    select
        *
    from {{ source('product_offering_explorer', 'churn_products') }}
)

, active_records as (
    select
        *
    from base
    where star_delete_time is null
)

, exclude_migrations_feb_2025 as (

    select
        ac.*
    from active_records ac
    left join {{ source('product_offering_explorer', 'reporting_channel_hierarchy') }} as ch
        on ac.star_prim_rpt_chnnl_id = ch.star_rpt_channel_hierarchy_id
    where
        ac.star_date_id_churn >= 20250201 ---fix start date id
        and
        (
            case
                when ch.reporting_channel_lvl_4_desc = 'Telesales' and ch.reporting_channel_lvl_3_desc = 'Other' then 'Y'
                when ch.reporting_channel_lvl_4_desc = 'Internal' and ch.reporting_channel_lvl_3_desc = 'Other' then 'Y'
                else 'N'
            end
        ) = 'N' --to exclude technical migrations of Ent customers  

    union all

    select
        ac.*
    from active_records ac
    where
        star_date_id_churn < 20250201
)

, product_mapping as (
    select
        case
            ------------------------------------------------------------------
            -- COMBO PRODUCTS
            ------------------------------------------------------------------
            when product in (
                'Combo Netflix & Streamz', 'Combo Netflix & Streamz B2B'
            ) then 'Combo Netflix & Streamz'

            when product in (
                'Combo Netflix & Streamz+', 'Combo Netflix & Streamz+ B2B'
            ) then 'Combo Netflix & Streamz+'

            when product in (
                'Combo Netflix & Streamz Premium', 'Combo Netflix & Streamz Premium B2B'
            ) then 'Combo Netflix & Streamz Premium'

            when product in (
                'Combo Netflix & Streamz Premium+', 'Combo Netflix & Streamz Premium+ B2B'
            ) then 'Combo Netflix & Streamz Premium+'

            when product in (
                'Combo Netflix & Play More', 'Combo Netflix & Play More B2B'
            ) then 'Combo Netflix & Play More'

            ----------------------------------------------------------
            -- STREAMZ FAMILY  (groupable under "Streamz tiers")
            -- Prior ORIGIN migration names + post migration names
            ----------------------------------------------------------
            -- prior ORIGIN migration
            when
                product in ('Streamz', 'Streamz B2B')
                and star_date_id_churn < 20240101 --- TODO: verify if churn date is used here as well?
                then 'Streamz Premium'

            when
                product in ('Streamz+', 'Streamz+ B2B')
                and star_date_id_churn < 20240101
                then 'Streamz Premium+'

            when
                product in ('Play More', 'Play More B2B')
                and star_date_id_churn < 20240101
                then 'Play More'

            -- post ORIGIN migration
            when
                product in ('Streamz Basic', 'Streamz Basic B2B')
                and star_date_id_churn >= 20240101
                then 'Streamz Basic'

            when
                product in ('Streamz Premium', 'Streamz Premium B2B')
                and star_date_id_churn >= 20240101
                then 'Streamz Premium'

            when
                product in ('Streamz Premium+', 'Streamz Premium+ B2B')
                and star_date_id_churn >= 20240101
                then 'Streamz Premium+'

            ----------------------------------------------------------
            -- DISNEY+ FAMILY (groupable under "Disney+ tiers")
            -- Use star_date_id_churn to handle rename in Oct 2024
            ----------------------------------------------------------
            -- Before Oct 2024 the premium tier appears as "Disney+"
            when
                product in ('Disney+', 'Disney+ B2B')
                and star_date_id_churn < 20241001
                then 'Disney+ Premium'

            -- After rename, premium appears as "Disney+ Premium"
            when
                product in ('Disney+ Premium', 'Disney+ Premium B2B')
                and star_date_id_churn >= 20241001
                then 'Disney+ Premium'

            -- Disney+ Standard – new tier from Sept 2025
            when product in ('Disney+ Standard', 'Disney+ Standard B2B')
                then 'Disney+ Standard'

            ----------------------------------------------------------
            -- NETFLIX FAMILY
            ----------------------------------------------------------
            when product in ('Netflix Basic', 'Netflix Basic B2B')
                then 'Netflix Basic'

            when product in ('Netflix Standard', 'Netflix Standard B2B')
                then 'Netflix Standard'

            when product in ('Netflix Premium', 'Netflix Premium B2B')
                then 'Netflix Premium'

            ----------------------------------------------------------
            -- DEFAULT: keep original name if nothing matches
            ----------------------------------------------------------
            else product
        end as product
        , * exclude (product)

    from exclude_migrations_feb_2025
)

, customer_id_mapping as (
    select
        COALESCE(payer_nc_cust_nbr, payer_cust_nbr) as customer_number
        , * exclude (payer_nc_cust_nbr, payer_cust_nbr)

    from product_mapping
)

, customer_category_group_mapping as (
    select
        case customer_category_group when 'BUS' then 'BUS' else 'RES' end as customer_category_group
        , * exclude (customer_category_group)

    from customer_id_mapping
)

select
    *
from customer_category_group_mapping
