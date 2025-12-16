----------------------------------------------------------------------------------------
--Q7 : What is the share of TV shop's sales in the last 3 months
----------------------------------------------------------------------------------------
with digital_sales as (
    -- Base dataset: all digital sales in Q1 2025
    select
        s.nr_sales
        , rch.reporting_channel_lvl_4_desc
    from {{ source('product_offering_explorer', 'sales_products') }} s
    inner join {{ source('product_offering_explorer', 'reporting_channel_hierarchy') }} rch
        on s.star_prim_rpt_chnnl_id = rch.star_rpt_channel_hierarchy_id
    left join {{ source('product_offering_explorer', 'd_date') }} d
        on s.star_date_id = d.star_date_id
    where
        s.star_delete_time is null
        and d.month between '2025-09' and '2025-11'
        and s.product_type = 'Content Product'
        and rch.reporting_channel_lvl_1_desc = 'Digital'
        -- Exclude technical migrations (Feb-Mar 2025)
        and not (
            s.star_date_id between 20250201 and 20250331
            and rch.reporting_channel_lvl_4_desc in ('Telesales', 'Internal')
            and rch.reporting_channel_lvl_3_desc = 'Other'
        )
        and s.product not ilike '%combo%'
)

, sales_aggregated as (
    -- Aggregate TV Shop vs total digital sales
    select
        sum(case when reporting_channel_lvl_4_desc ilike '%tv shop%' then nr_sales else 0 end) as tv_shop_sales
        , sum(nr_sales) as total_digital_sales
    from digital_sales
)

select
    tv_shop_sales
    , total_digital_sales
    , round(tv_shop_sales / nullif(total_digital_sales, 0) * 100, 2) as tv_shop_share_pct
from sales_aggregated
