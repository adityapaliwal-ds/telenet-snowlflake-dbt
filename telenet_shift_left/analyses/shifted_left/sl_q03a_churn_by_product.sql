----------------------------------------------------------------------
--Q3a : How many Streamz churn did we have in month September 2025?
----------------------------------------------------------------------

select
    -sum(nr_churn) as total_churn --to show positive values for churn

from {{ ref('clean__churn_products') }} c
left join {{ ref('d_date') }} d
    on c.star_date_id_churn = d.star_date_id

where
    d.month = '2025-09'
    and product_type = 'Content Product'
    and cleaned_entertainment_product in --specify the products here (using cleaned names without B2B suffix)
        (
            'Streamz Basic', 'Streamz Basic B2B',
            'Streamz Premium', 'Streamz Premium B2B',
            'Streamz Premium+', 'Streamz Premium+ B2B'
        ) 
group by all
order by 1
