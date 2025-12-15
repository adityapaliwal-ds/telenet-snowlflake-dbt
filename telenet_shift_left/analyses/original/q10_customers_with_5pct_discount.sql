--------------------------------------------------------------------------------
--Q10 : How many customers have 5% discount?  (insight available in one dataset)
--------------------------------------------------------------------------------
--Rule : As of Marketplace in 2025, customers with at least 2 entertainment products in their content mix
--get 5% discount on their total entertainment spend.

select
    month,
    count(distinct customer_number) as nbr_customers_with_5pct_discount
from {{ source('value_reporting_engine', 'active_inst_base_customers_tln') }}
where 1=1
  and star_delete_time is null
  and lower(cust_cust_cat_group_desc) <> 'tcc'
  -- and most_recent_snapshot = '1'--to provide most recent snapshot only
  and month = '2025-10'--specify the month here for a month specific insight
  and has_content = 'Y'
  and cont_mix like '%+%' --to filter customers with at least 2 entertainment products in content mix
group by month
order by 1
