------------------------------------------------------------------------------------------------
--Q12 : How many CAM CI+ customers/devices do we still have? (insight available in one dataset)
------------------------------------------------------------------------------------------------

select
    star_snapshot_month_id as snapshot_month,
    cleaned_device_categories as device_categories,
    count(distinct customer_number) as nbr_customers, -- Number of customers with CAM CI+ devices using most recent snapshot
    count(distinct device_serial_val) as nbr_devices -- Number of CAM CI+ devices using most recent snapshot
from {{ ref('clean__active_inst_base_stb_month') }}
where 1=1
    and most_recent_snapshot = '1' --to provide most recent snapshot only
    and device_product_type in ('CAM1300', 'CI0500-TNO01-31') --specify the device type here
group by 1, 2
order by 1, 2
