---------------------------------------------------------------------------------------------------------------------------------
--Q18 : What is the number of Play Sports customers per set-top box type? (Sipadan, EOS, Apollo) (join between multiple datasets)
---------------------------------------------------------------------------------------------------------------------------------

select
    stb.star_snapshot_month_id as snapshot_month,
    case
        when stb.device_product_type in ('2008C-STB-TN', 'DCX960') then 'EOS'
        when stb.device_product_type in ('CAM1300', 'CI0500-TNO01-31') then 'CAM CI+'
        when stb.device_product_type = 'DTVBC_APPEARTV' then 'DTV Broadcast Center' --TV Screens in TLN Headquarters used for tests and/or demos
        when stb.device_product_type like 'AD%' then 'SIPADAN'
        when stb.device_product_type = 'VIP7002W' then 'APOLLO'
    end as devices_type,
    count(distinct stb.customer_number) as nbr_customers
from {{ source('product_offering_explorer', 'active_inst_base_stb_month') }} stb
left join {{ source('value_reporting_engine', 'active_inst_base_customers_tln') }} ib
on stb.customer_number = ib.customer_number
and cast(replace(ib.month, '-', '') as integer) = stb.star_snapshot_month_id
where 1=1
    and stb.star_delete_time is null
    and lower(cust_cust_cat_group_desc) <> 'tcc'
    -- and ib.month between '2025-09' and '2025-11' --specify the month here
    and ib.month between '2025-09' and '2025-11' --specify the month here
    and
        (
            ib.play_sports_volume > 0 --to filter only Play Sports customers
            --Uncomment below depending on product asked by business
            --or ib.play_more_volume > 0 --to filter only Play More customers
            --or ib.streamz_volume > 0 --to filter only Streamz Premium customers
            --or ib.streamz_plus_volume > 0 --to filter only Streamz Premium+ customers
            --or ib.streamz_basic_volume > 0 --to filter only Streamz Basic customers
            --or ib.netflix_basic_volume > 0 --to filter only Netflix Basic customers
            --or ib.netflix_standard_volume > 0 --to filter only Netflix Standard customers
            --or ib.netflix_premium_volume > 0 --to filter only Netflix Premium customers
            --or ib.disney_plus_volume > 0 --to filter only Disney+ Standard & Premium customers
        )

group by 1, 2
order by 2,1
