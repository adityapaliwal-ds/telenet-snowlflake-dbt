----------------------------------------------------------------------
--Q3b :  How many Streamz sales did we have in month September 2025
----------------------------------------------------------------------

select sum(nr_sales) as total_sales

from {{ ref('clean__sales_products') }} s
left join {{ ref('d_date') }} d
    on s.star_date_id = d.star_date_id

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
