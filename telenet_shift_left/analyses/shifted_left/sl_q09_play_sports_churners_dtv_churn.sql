-------------------------------------------------------------------------------------------------------------------
--Q9 : How many e.g. Play Sports churners also churned their DTV line in month x? (with a join between 2 datasets)
-------------------------------------------------------------------------------------------------------------------

select
    churn_month,
    case
        when rgu_mix_before_churn like '%ETV%' and rgu_mix_after_churn not like '%ETV%' then 'Churned DTV & Play Sports'
        else 'Play Sports Only Churners'
    end as churn_type,
    count(distinct cleaned_customer_number) as nbr_churners
from {{ ref('clean__churn_products') }} c
left join {{ ref('d_date') }} d
    on c.star_date_id_churn = d.star_date_id
where 1=1
    --monthly aggregation
    and churn_month between '2025-09' and '2025-11' --specify the month here
    and cleaned_entertainment_product in ('Play Sports', 'Play Sports B2B') --to filter on a specific product using cleaned product names
group by 1,2
order by 1,2
