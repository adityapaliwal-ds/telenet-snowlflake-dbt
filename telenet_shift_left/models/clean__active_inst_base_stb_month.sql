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
        end as cleaned_device_categories
    from active_records
),

cleaned_month as (
    select
        *,
        substr(star_snapshot_month_id, 1, 4) || '-' || substr(star_snapshot_month_id, 5, 2) as cleaned_star_snapshot_month_id
    from device_categories
)

, final as (
    select
        *,
        -- make a composite primary key using customer_number and cleaned_star_snapshot_month_id
        customer_number || '_' || cleaned_star_snapshot_month_id as cleaned_composite_primary_key
        from cleaned_month
)

select 
    *
from final