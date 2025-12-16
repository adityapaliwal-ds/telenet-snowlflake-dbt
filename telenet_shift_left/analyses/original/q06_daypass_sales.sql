-------------------------------------------------------------------------------
--Q6 : How many day-passes did we sell within the last three months?
-------------------------------------------------------------------------------

select sum(s.nr_sales) as total_sales

from {{ source('product_offering_explorer', 'sales_products') }} s
left join {{ source('product_offering_explorer', 'reporting_channel_hierarchy') }} rch
    on s.star_prim_rpt_chnnl_id = rch.star_rpt_channel_hierarchy_id
left join {{ source('product_offering_explorer', 'd_date') }} d
    on s.star_date_id = d.star_date_id
where
    s.star_delete_time is null
    and not (
        s.star_date_id between 20250201 and 20251231
        and rch.reporting_channel_lvl_4_desc in ('Telesales', 'Internal')
        and rch.reporting_channel_lvl_3_desc = 'Other'

    )--To exclude technical migrations sales that happened in 2025  

    and d.month between '2025-09' and '2025-11' --specify the period range here
    and lower(s.product) like '%play sports%pass%' --Play Sports daypass naming patterns
