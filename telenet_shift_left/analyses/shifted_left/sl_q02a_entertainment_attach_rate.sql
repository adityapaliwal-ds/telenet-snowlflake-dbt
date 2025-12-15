----------------------------------------------------------------------------
--Q2 : What is the entertainment attach rate of the last month?
----------------------------------------------------------------------------

--All TLN HH with Entertainent content / All TLN HH
select
    month,
    count(distinct case when has_content = 'Y' then star_enr_mix_base_id end) as hh_with_content,
    count(distinct star_enr_mix_base_id) as total_hh,
    hh_with_content / nullif(total_hh, 0) as entertainment_attach_rate
from {{ ref('clean__active_inst_base_households_tln') }}
where
    month = '2025-11'
group by all
order by 1
