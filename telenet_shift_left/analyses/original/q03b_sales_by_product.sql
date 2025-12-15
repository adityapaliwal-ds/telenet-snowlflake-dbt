----------------------------------------------------------------------
--Q3b : How many Streamz sales did we have in in the last 3 months?
----------------------------------------------------------------------

select sum(s.nr_sales) as total_sales

from {{ source('product_offering_explorer', 'sales_products') }} s
left join {{ source('product_offering_explorer', 'reporting_channel_hierarchy') }} rch
    on s.star_prim_rpt_chnnl_id = rch.star_rpt_channel_hierarchy_id

where
    s.star_delete_time is null
    and s.sales_month between '2025-09' and '2025-11' --specify the period range here
    and s.product_type = 'Content Product'
    and not (--To exclude technical migrations sales that happened in 2025
        s.star_date_id between 20250201 and 20251231 --Migrations happened in 2025 from February to December
        and rch.reporting_channel_lvl_4_desc in ('Telesales', 'Internal')
        and rch.reporting_channel_lvl_3_desc = 'Other'
    )
    and product in --specify the products here
        (
            'Streamz Basic', 'Streamz Basic B2B',
            'Streamz Premium', 'Streamz Premium B2B',
            'Streamz Premium+', 'Streamz Premium+ B2B',
            'Play More', 'Play More B2B',
            'Play Sports', 'Play Sports B2B',
            'Netflix Basic', 'Netflix Basic B2B',
            'Netflix Standard', 'Netflix Standard B2B',
            'Netflix Premium', 'Netflix Premium B2B',
            'Disney+ Standard', 'Disney+ Standard B2B',
            'Disney+ Premium', 'Disney+ Premium B2B'
        ) 
group by all
order by 1
