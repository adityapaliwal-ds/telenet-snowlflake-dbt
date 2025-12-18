--------------------------------------------------------------------------------------------------------------------
--Q8a : What is the profile of our non-DTV customers? (Lifestage split)
--------------------------------------------------------------------------------------------------------------------
--Q8a : What is the lifestage profile of our non-DTV customers in October 2025?


select
    c.month
    , l.lifestage
    , count(distinct c.customer_number) as nbr_customers
from {{ ref('clean__active_inst_base_customers_tln') }} c
left join {{ ref('clean__lifestage_model_predictions') }} l
    on cast(c.customer_number as number) = l.customernumber
    and c.month = l.cleaned_month_code
where
    1 = 1
    and c.month between '2025-09' and '2025-11'--Important note is that the dataset is refreshed monthly before 15th of each month for the previous month
    and c.has_content = 'Y'
    and c.has_dtv = 'N' --non-DTV customers
group by 1, 2
order by 1, 2
