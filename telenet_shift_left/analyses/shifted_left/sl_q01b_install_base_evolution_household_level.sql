----------------------------------------------------------------------------------------
--Q1b : Household level install base evolution
--What is the evolution of install base for 2025-09 til 2025-11 on a household level?
----------------------------------------------------------------------------------------

select
    month
    , count(distinct star_enr_mix_base_id) as install_base_households --star_enr_mix_base_id is HH ID
from {{ ref('clean__active_inst_base_households_tln') }}
where
    1 = 1
    and month between '2025-09' and '2025-11' --specify the period range here
    and has_content = 'Y' --if we want all HH with content subscriptions without specific product filter
group by all
order by 1
