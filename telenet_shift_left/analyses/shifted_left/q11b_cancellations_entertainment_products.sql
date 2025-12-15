----------------------------------------------------------------------------------------------------
--Q11b : How many entertainment product cancellations did we have in the last x months?
----------------------------------------------------------------------------------------------------

select
    d.month as cancel_month,
    pp.cleaned_entertainment_product as entertainment_product,
    sum(nap.reg_org_cncl_net_add_prd_no) as total_cancellations
from {{ ref('clean__registered_net_adds_products') }} nap
left join {{ ref('clean__d_mts_enrollment_product') }} pp on nap.star_product_id=pp.star_enrollment_product_id
left outer join {{ ref('d_date') }} d on nap.star_transaction_date_id=d.star_date_id
where 1=1
    and nap.star_transaction_date_id between 20251101 and 20251130 --specify the month here
    and pp.cleaned_entertainment_product in --specify the products here (using cleaned names without B2B suffix)
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
order by 1, 2
