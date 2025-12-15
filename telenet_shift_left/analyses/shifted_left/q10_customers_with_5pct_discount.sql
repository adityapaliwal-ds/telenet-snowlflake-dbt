--------------------------------------------------------------------------------
--Q10 : How many customers have 5% discount?  (insight available in one dataset)
--------------------------------------------------------------------------------
--Rule : As of Marketplace in 2025, customers with at least 2 entertainment products in their content mix
--get 5% discount on their total entertainment spend.

select
    month,
    count(distinct customer_number) as nbr_customers_with_5pct_discount
from {{ ref('clean__active_inst_base_customers_tln') }}
where 1=1
  and month = '2025-10'--specify the month here for a month specific insight
  and has_content = 'Y'
  and cont_mix like '%+%' --to filter customers with at least 2 entertainment products in content mix
group by 1
order by 1
