-------------------------------------------------------------------------------------------------------------------
--Q9 : How many e.g. Play Sports churners also churned their DTV line in month x? (with a join between 2 datasets)
-------------------------------------------------------------------------------------------------------------------

select
    churn_month,
    case
        when rgu_mix_before_churn like '%ETV%' and rgu_mix_after_churn not like '%ETV%' then 'Churned DTV & Play Sports'
        else 'Play Sports Only Churners'
    end as churn_type,
    count(coalesce(payer_nc_cust_nbr,payer_cust_nbr)) as nbr_churners
from {{ source('product_offering_explorer', 'churn_products') }} c
left join {{ source('product_offering_explorer', 'reporting_channel_hierarchy') }} rch
on c.star_prim_rpt_chnnl_id = rch.star_rpt_channel_hierarchy_id
where 1=1
    and c.star_delete_time is null
    --monthly aggregation
    and churn_month = '2025-11' --specify the month here
    --and churn_month between '2025-01' and '2025-11' --specify the period range here
    and product in ('Play Sports',  'Play Sports B2B') --to filter on a specific product using below products mapping
    and not (--To exclude technical migrations churns that happened in 2025
        star_date_id_churn between 20250201 and 20251231 --Migrations happened in 2025 from February to December
        and reporting_channel_lvl_4_desc in ('Telesales', 'Internal')
        and reporting_channel_lvl_3_desc = 'Other'
    )
group by 1,2
order by 1,2
