with base as (
    select
        *
    from {{ source('product_offering_explorer', 'active_inst_base_stb_month') }}
),

active_records as (
    select
        *
    from base
    where star_delete_time is null
),

device_categories as (
    select
        *
      , case
            when device_product_type in ('2008C-STB-TN', 'DCX960') then 'EOS'
            when device_product_type in ('CAM1300', 'CI0500-TNO01-31') then 'CAM CI+'
            when device_product_type = 'DTVBC_APPEARTV' then 'DTV Broadcast Center'
            when device_product_type like 'AD%' then 'SIPADAN'
            when device_product_type = 'VIP7002W' then 'APOLLO'
        end as device_categories
    from active_records
)

select 
    *
from device_categories