----------------------------------------------------------------------------------------
--Q7 : What is the share of TV shop's sales in digital sales in Q1 of 2025?
----------------------------------------------------------------------------------------
with digital_sales as (
    -- Base dataset: all digital sales in Q1 2025
    select
        s.nr_sales
        , rch.reporting_channel_lvl_4_desc
    from {{ ref('clean__sales_products') }} s
    inner join {{ ref('clean__reporting_channel_hierarchy') }} rch
        on s.star_prim_rpt_chnnl_id = rch.star_rpt_channel_hierarchy_id
    where
        s.sales_month between '2025-01' and '2025-03'
        and s.product_type = 'Content Product'
        and rch.reporting_channel_lvl_1_desc = 'Digital'
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
