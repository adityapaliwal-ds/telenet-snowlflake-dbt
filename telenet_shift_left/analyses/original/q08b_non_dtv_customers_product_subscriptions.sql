--------------------------------------------------------------------------------------------------------------------
--Q8b : What is the product subscriptions profile of our non-DTV customers in October 2025? (Product subscriptions)
--------------------------------------------------------------------------------------------------------------------

select
    month,
    case
        when streamz_basic_volume > 0 then 'Streamz Basic'
        when streamz_volume > 0 then 'Streamz Premium'
        when streamz_plus_volume > 0 then 'Streamz Premium+'
        -- when play_more_volume > 0 then 'Play More'
        -- when play_sports_volume > 0 then 'Play Sports'
        -- when netflix_basic_volume > 0 then 'Netflix Basic'
        -- when netflix_standard_volume > 0 then 'Netflix Standard'
        -- when netflix_premium_volume > 0 then 'Netflix Premium'
        -- when disney_plus_volume > 0 then 'Disney+ Standard & Premium'
        -- when disney_
    end as product,
    count(distinct customer_number) as nbr_customers
from {{ source('value_reporting_engine', 'active_inst_base_customers_tln') }}
where 1=1
  and lower(cust_cust_cat_group_desc) <> 'tcc'
  and star_delete_time is null
  and month = '2025-10'
  -- and month between '2025-10' and '2025-12'
  -- and has_content = 'Y'
  and has_dtv = 'N' --non-DTV customers
group by 1, 2
order by 1, 2
