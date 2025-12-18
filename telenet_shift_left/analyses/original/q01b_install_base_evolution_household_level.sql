----------------------------------------------------------------------------------------
--Q1b : Household level install base evolution
--What is the evolution of install base for households for the last 3 months? 
----------------------------------------------------------------------------------------

select
    d.month
    , count(distinct star_enr_mix_base_id) as install_base_households --star_enr_mix_base_id is HH ID
from {{ source('value_reporting_engine', 'active_inst_base_households_tln') }} h
left join {{ source('product_offering_explorer', 'd_date') }} d
    on h.star_date_id = d.star_date_id
where
    h.star_delete_time is null
    and lower(hh_cust_cat_group_desc) <> 'tcc'

    and d.month between '2025-09' and '2025-11' --specify the period range here
    and has_content = 'Y' --if we want all HH with content subscriptions without specific product filter
group by all
order by 1
