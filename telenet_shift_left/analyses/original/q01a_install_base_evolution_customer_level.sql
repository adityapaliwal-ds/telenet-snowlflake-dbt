----------------------------------------------------------------------------------------
--Q1a : Customer level install base evolution
--What is the evolution of install base for 2025-09 til 2025-11 on a customer level?
----------------------------------------------------------------------------------------

select
    month
    , count(customer_number) as install_base_customers
from {{ source('value_reporting_engine', 'active_inst_base_customers_tln') }}
where
    star_delete_time is null
    and lower(cust_cust_cat_group_desc) <> 'tcc'
    and month between '2025-09' and '2025-11' --specify the period range here
    and has_content = 'Y' --if we want all HH with content subscriptions without specific product filter
group by all
order by month
