--------------------------------------------------------------------------------------------------------------------
--Q8c : What is the profile of our non-DTV customers? (Behavioral segmentation)
--------------------------------------------------------------------------------------------------------------------

select
    c.month,
    res.predictions as res_behavior_segment,
    count(distinct c.customer_number) as nbr_customers
from {{ ref('clean__active_inst_base_customers_tln') }} c
left join {{ ref('clean__tln_behav_segm_model_predictions') }} res
on cast(c.customer_number as number) = res.customernumber
and c.month = res.cleaned_month_code
where 1=1
  and c.month = '2025-11'--Important note is that the dataset is refreshed monthly before 15th of each month for the previous month
  and c.has_content = 'Y'
  and c.has_dtv = 'N' --non-DTV customers
group by 1, 2
order by 1, 2
