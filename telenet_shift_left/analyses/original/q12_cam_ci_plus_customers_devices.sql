------------------------------------------------------------------------------------------------
--Q12 : How many CAM CI+ customers/devices do we still have? (insight available in one dataset)
------------------------------------------------------------------------------------------------

select
    star_snapshot_month_id as snapshot_month,
    case
        when device_product_type in ('2008C-STB-TN', 'DCX960') then 'EOS'
        when device_product_type in ('CAM1300', 'CI0500-TNO01-31') then 'CAM CI+'
        when device_product_type = 'DTVBC_APPEARTV' then 'DTV Broadcast Center' --TV Screens in TLN Headquarters used for tests and/or demos
        when device_product_type like 'AD%' then 'SIPADAN'
        when device_product_type = 'VIP7002W' then 'APOLLO'
    end as device_categories,
    count(distinct customer_number) as nbr_customers, -- Number of customers with CAM CI+ devices using most recent snapshot
    count(distinct device_serial_val) as nbr_devices -- Number of CAM CI+ devices using most recent snapshot
from {{ source('product_offering_explorer', 'active_inst_base_stb_month') }}
where 1=1
    and star_delete_time is null
    and most_recent_snapshot = '1' --to provide most recent snapshot only
    and device_product_type in ('CAM1300', 'CI0500-TNO01-31') --specify the device type here
group by 1, 2
order by 1, 2
