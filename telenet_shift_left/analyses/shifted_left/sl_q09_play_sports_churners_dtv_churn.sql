-------------------------------------------------------------------------------------------------------------------
--Q9 : How many e.g. Play Sports churners also churned their DTV line in month x? (with a join between 2 datasets)
-------------------------------------------------------------------------------------------------------------------

select
    churn_month,
    case
        when rgu_mix_before_churn like '%ETV%' and rgu_mix_after_churn not like '%ETV%' then 'Churned DTV & Play Sports'
        else 'Play Sports Only Churners'
    end as churn_type,
    count(cleaned_customer_number) as nbr_churners
from {{ ref('clean__churn_products') }}
where 1=1
    --monthly aggregation
    and churn_month = '2025-11' --specify the month here
    --and churn_month between '2025-01' and '2025-11' --specify the period range here
    and cleaned_entertainment_product = 'Play Sports' --to filter on a specific product using cleaned product names
group by 1,2
order by 1,2
