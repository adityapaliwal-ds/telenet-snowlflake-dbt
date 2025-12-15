----------------------------------------------------------------------------
--Q2 : What is the entertainment attach rate of the last month for households with DTV?
----------------------------------------------------------------------------


--All DTV HH with Entertainent content / All DTV HH
select
    month,
    count(distinct case when has_content = 'Y' and has_dtv = 'Y' then star_enr_mix_base_id end) as dtv_hh_with_content,
    count(distinct case when has_dtv = 'Y' then star_enr_mix_base_id end) as total_dtv_hh,
    dtv_hh_with_content / nullif(total_dtv_hh, 0) as entertainment_attach_rate
from {{ source('value_reporting_engine', 'active_inst_base_households_tln') }}
where
    star_delete_time is null
    and lower(hh_cust_cat_group_desc) <> 'tcc'
    and month = '2025-11'
group by all
order by 1
