--------------------------------------------------------------------------------------------------------------------
--Q8a : What is the profile of our non-DTV customers? (Lifestage split)
--------------------------------------------------------------------------------------------------------------------
--Q8a : What is the lifestage profile of our non-DTV customers in October 2025?


select
    c.month
    , l.lifestage
    , count(distinct c.customer_number) as nbr_customers
from {{ source('value_reporting_engine', 'active_inst_base_customers_tln') }} c
left join {{ source('customer_profiler', 'lifestage_model_predictions') }} l
    on cast(c.customer_number as number) = l.customernumber
    and to_number(replace(c.month, '-', '')) = l.month_code
where
    1 = 1
    and lower(c.cust_cust_cat_group_desc) <> 'tcc'
    and c.star_delete_time is null
    and c.month = '2025-10'--Important note is that the dataset is refreshed monthly before 15th of each month for the previous month
    and c.has_content = 'Y'
    and c.has_dtv = 'N' --non-DTV customers
group by 1, 2
order by 1, 2
