----------------------------------------------------------------------------------------
--Q4 : What is the number of Play Sports' churn for the digital sales channel in the last 13 months?
----------------------------------------------------------------------------------------

--NB :
--1. Always use the the reporting sales/churns channels fields in {{ source('product_offering_explorer', 'reporting_channel_hierarchy') }}
--2. Recommended to use levels 1 only because levels 1 to 4 are too much detailed for business. Provide other levels only upon request.

select
    rch.reporting_channel_lvl_1_desc as sales_channl
    , -sum(c.nr_churn) as total_churn

from {{ source('product_offering_explorer', 'churn_products') }} c
left join {{ source('product_offering_explorer', 'reporting_channel_hierarchy') }} rch
    on c.star_prim_rpt_chnnl_id = rch.star_rpt_channel_hierarchy_id
where
    c.star_delete_time is null
    and c.churn_month between '2024-10' and '2025-11' --specify the period range here
    and c.product in ('Play Sports', 'Play Sports B2B') --to filter on a specific product using below products mapping
    and not (--To exclude technical migrations churns that happened in 2025
        c.star_date_id_churn between 20250201 and 20251231 --Migrations happened in 2025 from February to December
        and rch.reporting_channel_lvl_4_desc in ('Telesales', 'Internal')
        and rch.reporting_channel_lvl_3_desc = 'Other'
    )
group by all
order by total_churn desc
