----------------------------------------------------------------------------------------------------
--Q11b : How many entertainment product cancellations did we have in the last x months?
----------------------------------------------------------------------------------------------------

select
    d.month as cancel_month,
    case
        when pp.product_desc in ('Streamz Basic',  'Streamz Basic B2B') then 'Streamz Basic'
        when pp.product_desc in ('Streamz Premium',  'Streamz Premium B2B') then 'Streamz Premium'
        when pp.product_desc in ('Streamz Premium+',  'Streamz Premium+ B2B') then 'Streamz Premium+'
        when pp.product_desc in ('Play More',  'Play More B2B') then 'Play More'
        when pp.product_desc in ('Play Sports',  'Play Sports B2B') then 'Play Sports'
        when pp.product_desc in ('Netflix Basic',  'Netflix Basic B2B') then 'Netflix Basic'
        when pp.product_desc in ('Netflix Standard',  'Netflix Standard B2B') then 'Netflix Standard'
        when pp.product_desc in ('Netflix Premium',  'Netflix Premium B2B') then 'Netflix Premium'
        when pp.product_desc in ('Disney+ Standard',  'Disney+ Standard B2B') then 'Disney+ Standard'
        when pp.product_desc in ('Disney+ Premium',  'Disney+ Premium B2B') then 'Disney+ Premium'
    end as entertainment_product,
    sum(nap.reg_org_cncl_net_add_prd_no) as total_cancellations
from {{ source('product_offering_explorer', 'registered_net_adds_products') }} nap
left join {{ source('product_offering_explorer', 'reporting_channel_hierarchy') }} rch on nap.star_prim_rpt_chnnl_id = rch.star_rpt_channel_hierarchy_id
left join {{ source('product_offering_explorer', 'd_mts_enrollment_product') }} pp on nap.star_product_id=pp.star_enrollment_product_id
left outer join {{ source('product_offering_explorer', 'd_date') }} d on nap.star_transaction_date_id=d.star_date_id
where 1=1
    and nap.star_delete_time is null
    and nap.star_transaction_date_id between 20251101 and 20251130 --specify the month here
    and pp.product_desc in --specify the products here
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

    and not (--To exclude technical migrations churns that happened in 2025
        nap.star_transaction_date_id between 20250201 and 20251231 --Migrations happened in 2025 from February to December
        and rch.reporting_channel_lvl_4_desc in ('Telesales', 'Internal')
        and rch.reporting_channel_lvl_3_desc = 'Other'
    )

group by all
order by 1, 2
