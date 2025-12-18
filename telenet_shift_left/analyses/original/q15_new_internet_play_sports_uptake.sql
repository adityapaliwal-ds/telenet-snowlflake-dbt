------------------------------------------------------------------------------------------------------------------------------
--Q15: Amongst new internet lines since e.g. September, how many subscribed to e.g. Play Sports within the following 2 months?
--Q15b : How many subscribed through a promo? (becomes RED question and should be redirected to analysts!)
------------------------------------------------------------------------------------------------------------------------------

-- 1) Define the months in scope (e.g. from 2025-09 onwards)
with months as (
    select '2025-09' as month 
    
    -- union all
    -- select '2025-10' union all
    -- select '2025-11' union all
    -- select '2025-12'
),

-- 2) New Internet activations only (no Int before, Int after)
new_internet_activations as (
    select
        activation_month as internet_activation_month,
        coalesce(payer_nc_cust_nbr, payer_cust_nbr) as customer_number
    from {{ source('product_offering_explorer', 'activation_lines') }}
    where 1=1
      and star_delete_time is null
    --   and activation_month between '2025-09' and '2025-12'  -- adjust range here
    and activation_month = '2025-09'  -- adjust range here
      and rgu_mix_before_activation not like '%Int%'        -- no Internet before
      and rgu_mix_after_activation like '%Int%'             -- Internet after
),

-- 3) Play Sports base (customers with Play Sports active)
play_sports_base as (
    select
        month,
        customer_number,
        cont_mix
    from {{ source('value_reporting_engine', 'active_inst_base_customers_tln') }}
    where 1=1
      and star_delete_time is null
      and lower(cust_cust_cat_group_desc) <> 'tcc'
      and month between '2025-09' and '2026-11'  -- need to cover activation + 2 months window
      and (
            play_sports_volume > 0      -- adapt to correct field name(s) for Play Sports
      )
),

-- 4) First Play Sports subscription within 2 months after new Internet activation
play_sports_within_2m as (
    select
        a.customer_number,
        a.internet_activation_month,
        min(p.month) as first_play_sports_month
    from new_internet_activations a
    join play_sports_base p
      on p.customer_number = a.customer_number
     -- Play Sports must start between activation month and +2 months
     and p.month between
            a.internet_activation_month
        and to_char(
                add_months(to_date(a.internet_activation_month, 'YYYY-MM'), 2),
                'YYYY-MM'
            )  -- 2-month window (activation month + 2)
    group by a.customer_number, a.internet_activation_month
)

-- 5) Final: per activation month, how many new Internet lines and how many took Play Sports within 2 months?
select
    m.month,
    count(distinct a.customer_number) as new_internet_customers,
    count(distinct ps.customer_number) as playsports_within_2m
from months m
left join new_internet_activations a
       on a.internet_activation_month = m.month
left join play_sports_within_2m ps
       on ps.internet_activation_month = m.month
group by m.month
order by m.month
