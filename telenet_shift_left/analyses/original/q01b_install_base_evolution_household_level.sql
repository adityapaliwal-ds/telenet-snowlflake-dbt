----------------------------------------------------------------------------------------
--Q1b : Household level install base evolution
--What is the evolution of install base for period 2023-01 up until 2024-12
----------------------------------------------------------------------------------------

select
    month
    , count(distinct star_enr_mix_base_id) as install_base_households --star_enr_mix_base_id is HH ID
from {{ source('value_reporting_engine', 'active_inst_base_households_tln') }}
where
    star_delete_time is null
    and lower(hh_cust_cat_group_desc) <> 'tcc'

    and month between '2023-01' and '2024-12' --specify the period range here
    and has_content = 'Y' --if we want all HH with content subscriptions without specific product filter
group by all
order by 1
