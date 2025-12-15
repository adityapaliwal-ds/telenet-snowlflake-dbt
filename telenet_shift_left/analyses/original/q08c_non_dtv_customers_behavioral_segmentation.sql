--------------------------------------------------------------------------------------------------------------------
--Q8c : What is the profile of our non-DTV customers? (Behavioral segmentation)
--------------------------------------------------------------------------------------------------------------------

select
    month,
    res.predictions as res_behavior_segment,
    count(distinct customer_number) as nbr_customers
from {{ source('value_reporting_engine', 'active_inst_base_customers_tln') }} c
left join {{ source('customer_profiler', 'tln_behav_segm_model_predictions') }} res
on cast(c.customer_number as number) = res.customernumber
and to_number(replace(c.month, '-', '') )= res.month_code
where 1=1
  and lower(c.cust_cust_cat_group_desc) <> 'tcc'
  and c.star_delete_time is null
  and c.month = '2025-11'--Important note is that the dataset is refreshed monthly before 15th of each month for the previous month
  and c.has_content = 'Y'
  and c.has_dtv = 'N' --non-DTV customers
group by 1, 2
order by 1, 2
