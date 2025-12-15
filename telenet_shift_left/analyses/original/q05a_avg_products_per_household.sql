----------------------------------------------------------------------------
--Q5a : How many "entertainment" products on average per household for the last 3 months?
----------------------------------------------------------------------------


with per_month as (
    select
        month
        , sum(
            case
                when cont_mix = 'None' or cont_mix is null then 0
                else array_size(split(cont_mix, '+'))
            end
        ) / nullif(sum(nbr_hh_eop), 0) as avg_products_per_hh_month
    from {{ source('value_reporting_engine', 'active_inst_base_households_tln') }}
    where
        star_delete_time is null
        and lower(hh_cust_cat_group_desc) <> 'tcc'
        and month between '2025-09' and '2025-11'
        and has_content = 'Y'
    group by month
)

select avg(avg_products_per_hh_month) as avg_products_per_hh_over_months
from per_month
