----------------------------------------------------------------------------------------
--Q1a : Customer level install base evolution
--What is the evolution of install base for customers for the last 3 months? 
----------------------------------------------------------------------------------------

select
    d.month
    , count(customer_number) as install_base_customers
from {{ source('value_reporting_engine', 'active_inst_base_customers_tln') }} c
left join {{ source('product_offering_explorer', 'd_date') }} d
    on c.star_date_id = d.star_date_id
where
    c.star_delete_time is null
    and lower(cust_cust_cat_group_desc) <> 'tcc'
    and d.month between '2025-09' and '2025-11' --specify the period range here
    and has_content = 'Y' --if we want all HH with content subscriptions without specific product filter
group by all
order by d.month
