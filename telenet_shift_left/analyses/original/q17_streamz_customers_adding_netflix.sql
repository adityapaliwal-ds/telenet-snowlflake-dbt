-- ========================================================================================================================
-- Q17 - Replacement : How many customers already having Streamz in previous month take Netflix on top in current month and end-up in the Netflix-Streamz combo?
-- Technical translation : How many Streamz customers have transitioned from standalone STR products in M-1 to a combination of STR and NF products in M?
-- ========================================================================================================================

-- Full period analysis from January to December 2025 - monthly aggregation
with months as (
    -- Define the months to analyze
    select '2024-12' as month union all
    select '2025-01' union all
    select '2025-02' union all
    select '2025-03' union all
    select '2025-04' union all
    select '2025-05' union all
    select '2025-06' union all
    select '2025-07' union all
    select '2025-08' union all
    select '2025-09' union all
    select '2025-10' union all
    select '2025-11'
    -- select '2025-12'
),

streamz_base as (
    -- Monthly view of customers who have Streamz products (any tier)
    select
        month,
        customer_number,
        cont_mix
    from {{ source('value_reporting_engine', 'active_inst_base_customers_tln') }}
    where 1=1
      and star_delete_time is null
      and lower(cust_cust_cat_group_desc) <> 'tcc'
      and (
            streamz_basic_volume > 0
         or streamz_volume > 0
         or streamz_plus_volume > 0
      )
),

netflix_base as (
    -- Monthly view of customers who have Netflix products (any tier)
    select
        month,
        customer_number,
        cont_mix
    from {{ source('value_reporting_engine', 'active_inst_base_customers_tln') }}
    where 1=1
      and star_delete_time is null
      and lower(cust_cust_cat_group_desc) <> 'tcc'
      and (
            netflix_basic_volume > 0
         or netflix_standard_volume > 0
         or netflix_premium_volume > 0
      )
),

month_pairs as (
    -- Generate consecutive month pairs (previous month → current month)
    select
        m1.month as prev_month,
        m2.month as curr_month
    from months m1
    join months m2
      on date_trunc('month', to_date(m1.month || '-01', 'YYYY-MM-DD') + interval '1 month')
         = date_trunc('month', to_date(m2.month || '-01', 'YYYY-MM-DD'))
),

joined as (
    -- Join Streamz base (month m) with Netflix base (month m+1)
    select
        mp.prev_month,
        mp.curr_month,
        s.customer_number,
        s.cont_mix as cont_mix_prev,
        n.cont_mix as cont_mix_curr
    from month_pairs mp
    join streamz_base s
      on s.month = mp.prev_month
    left join netflix_base n
      on n.customer_number = s.customer_number
     and n.month = mp.curr_month
    where 1=1
      -- Keep only Streamz-only customers in month m
      and (
            (s.cont_mix in ('Streamz', 'Streamz Basic', 'Streamz Plus')) --standalone STR
            or (s.cont_mix not like '%Netflix%') --STR mixed with other products than Netflix
          )
)

-- Aggregate: count total Streamz-only and those who added NF in the next month
select
    j.prev_month as month,
    count(distinct j.customer_number) as streamz_without_netflix,
    count(distinct case
                      when j.cont_mix_curr like '%Netflix%'
                       and j.cont_mix_curr like '%Streamz%'
                      then j.customer_number
                  end) as nbr_nf_on_top_subscribers_m_plus_one
from joined j
group by j.prev_month
order by j.prev_month
