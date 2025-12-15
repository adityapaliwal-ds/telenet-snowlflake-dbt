----------------------------------------------------------------------
--Q3a : How many Streamz churn did we have in the last 3 months?
----------------------------------------------------------------------

select
    -sum(nr_churn) as total_churn --to show positive values for churn

from {{ ref('clean__churn_products') }}

where
    churn_month between '2025-09' and '2025-11' --specify the period range here
    and product_type = 'Content Product'
    and cleaned_entertainment_product in --specify the products here (using cleaned names without B2B suffix)
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
