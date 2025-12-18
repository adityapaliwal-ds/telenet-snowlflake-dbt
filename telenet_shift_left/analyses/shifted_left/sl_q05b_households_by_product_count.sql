----------------------------------------------------------------------------
--Q5b : How many households had more than 1/2/3 entertainment products in the last 3 months?
----------------------------------------------------------------------------

-- with last_3_months as (
--     select
--         star_enr_mix_base_id
--         , month
--         , case
--             when cont_mix is null or cont_mix = 'None' then 0
--             else array_size(split(cont_mix, '+'))
--         end as nbr_products_in_month
--     from {{ ref('clean__active_inst_base_households_tln') }}
--     where
--         month between '2025-09' and '2025-11'
-- )

-- , per_hh as (
--     select
--         star_enr_mix_base_id
--         , max(nbr_products_in_month) as max_prodcuts_in_a_month_3m
--     from last_3_months
--     group by star_enr_mix_base_id
-- )

-- select
--     count(*) as total_households
--     , sum(case when max_prodcuts_in_a_month_3m > 1 then 1 else 0 end) as hh_more_than_1_product
--     , sum(case when max_prodcuts_in_a_month_3m > 2 then 1 else 0 end) as hh_more_than_2_products
--     , sum(case when max_prodcuts_in_a_month_3m > 3 then 1 else 0 end) as hh_more_than_3_products
-- from per_hh


with last_3_months as (
    select
        star_enr_mix_base_id
        , month
        , case
            when cont_mix is null or cont_mix = 'None' then 0
            else array_size(split(cont_mix, '+'))
        end as nbr_products_in_month
    from {{ ref('clean__active_inst_base_households_tln') }}
    where
        month between '2025-09' and '2025-11'
)

select
    month,
    nbr_products_in_month,
    count(*) as total_households,
from last_3_months
where month = '2025-09' and nbr_products_in_month > 1
group by month, nbr_products_in_month


#TODO 