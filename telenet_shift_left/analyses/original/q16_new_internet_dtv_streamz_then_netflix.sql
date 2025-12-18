----------------------------------------------------------------------------------------------------
--Q16 : How many new Internet and/or DTV customers take Streamz immediately?
--And within the 12 months after taking Streamz, also take Netflix? (join between multiple datasets)
----------------------------------------------------------------------------------------------------
--NB : 'immediately' is defined here as within the first 3 months after new Internet and/or DTV activation.
--NB : 'within the 12 months after taking Streamz' is defined here as within the first 9 months after Streamz subscription (to keep overall timeframe within 1 year from activation).
--NB : Here I limit the insights to 2025 months for testing purpose, but we can go back to earlier years if needed or further limit the months range based on business inputs.

with months as (
    select '2025-01' as month union all
    select '2025-02' union all
    select '2025-03' union all
    select '2025-04' union all
    select '2025-05' union all
    select '2025-06' union all
    select '2025-07' union all
    select '2025-08' union all
    select '2025-09' union all
    select '2025-10' union all
    select '2025-11' union all
    select '2025-12'
),

new_internet_dtv_activations as (
    select
        activation_month as int_and_or_dtv_activation_month,
        coalesce(payer_nc_cust_nbr, payer_cust_nbr) as customer_number
    from {{ source('product_offering_explorer', 'activation_lines') }}
    where 1=1
      and star_delete_time is null
      and activation_month between '2025-01' and '2025-12' -- new internet and/or DTV activations in 2025 (months can be adjusted based on business needs)
      and (
            rgu_mix_before_activation not like '%Int%'
         and rgu_mix_before_activation not like '%ETV%'
      )-- did not have Internet and/or DTV customers before
      and (
            rgu_mix_after_activation like '%Int%'
         or rgu_mix_after_activation like '%ETV%'
      )-- new Internet and/or DTV customers after
      and brand = 'Telenet'
),



streamz_base as (
    select
        month,
        customer_number,
        cont_mix
    from {{ source('value_reporting_engine', 'active_inst_base_customers_tln') }}
    where 1=1
      and star_delete_time is null
      and lower(cust_cust_cat_group_desc) <> 'tcc'
      and month >= '2025-01' and month <= '2025-12'-- considering Streamz base within 2025 (months can be adjusted based on business needs)
      and (
            streamz_basic_volume > 0
         or streamz_volume > 0
         or streamz_plus_volume > 0
      )
),

netflix_base as (
    select
        month,
        customer_number,
        cont_mix
    from {{ source('value_reporting_engine', 'active_inst_base_customers_tln') }}
    where 1=1
      and star_delete_time is null
      and lower(cust_cust_cat_group_desc) <> 'tcc'
    and month >= '2025-01' and month <= '2025-12' -- considering Netflix base within 2025 (months can be adjusted based on business needs)
      and (
            netflix_basic_volume > 0
         or netflix_standard_volume > 0
         or netflix_premium_volume > 0
      )
),

-- Step 1: first Streamz within 3M after activation
streamz_within_3m as (
    select
        a.customer_number,
        a.int_and_or_dtv_activation_month,
        min(s.month) as first_streamz_month
    from new_internet_dtv_activations a
    join streamz_base s
      on s.customer_number = a.customer_number
     and s.cont_mix like '%Streamz%'
     and s.cont_mix not like '%Netflix%'
     and s.month between a.int_and_or_dtv_activation_month and to_char(add_months(to_date(a.int_and_or_dtv_activation_month, 'YYYY-MM'), 2), 'YYYY-MM')-- 3 months window
    group by a.customer_number, a.int_and_or_dtv_activation_month
),

-- Step 2: Netflix + Streamz within 9M after Streamz
netflix_on_top as (
    select
        s.customer_number,
        s.int_and_or_dtv_activation_month,
        s.first_streamz_month,
        min(n.month) as netflix_added_month
    from streamz_within_3m s
    join netflix_base n
      on n.customer_number = s.customer_number
     and n.cont_mix like '%Netflix%'
     and n.cont_mix like '%Streamz%'
     and n.month between s.first_streamz_month and to_char(add_months(to_date(s.first_streamz_month, 'YYYY-MM'), 8), 'YYYY-MM') -- 9 months window
    group by s.customer_number, s.int_and_or_dtv_activation_month, s.first_streamz_month
)

select
    m.month,
    count(distinct a.customer_number) as new_int_or_dtv_or_intplusdtv_customers,
    count(distinct s.customer_number) as streamz_within_3m,
    count(distinct n.customer_number) as netflix_on_top_within_9m
from months m
left join new_internet_dtv_activations a on a.int_and_or_dtv_activation_month = m.month
left join streamz_within_3m s on s.int_and_or_dtv_activation_month = m.month
left join netflix_on_top n on n.netflix_added_month = m.month
group by m.month
order by m.month
