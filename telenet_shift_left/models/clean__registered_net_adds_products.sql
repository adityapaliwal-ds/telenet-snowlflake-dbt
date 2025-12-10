with nap as (
    select 
        *
    from {{ source('product_offering_explorer', 'registered_net_adds_products') }}
)

, active_nap as (
    select
        *
    from nap
    where star_delete_time is null
)

, exclude_migrations_feb_2025 as (
    select 
        anap.*
    from active_nap anap
    left join {{ source('product_offering_explorer', 'reporting_channel_hierarchy') }} as rch
        on anap.star_prim_rpt_chnnl_id = rch.star_rpt_channel_hierarchy_id
    where 
        anap.star_transaction_date_id >= 20250201
        and 
        (
            case 
                when rch.reporting_channel_lvl_4_desc = 'Telesales' and rch.reporting_channel_lvl_3_desc = 'Other' then 'Y'
                when rch.reporting_channel_lvl_4_desc = 'Internal' and rch.reporting_channel_lvl_3_desc = 'Other' then 'Y'
                else 'N'
            end
        ) = 'N'
        
    union all

    select
        anap.*
    from active_nap anap
    where
        star_transaction_date_id < 20250201 --to exclude technical migrations of Ent customers
    )

select * from exclude_migrations_feb_2025