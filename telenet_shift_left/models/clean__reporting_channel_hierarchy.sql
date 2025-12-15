with source as (
    select *
        from {{ source('product_offering_explorer', 'reporting_channel_hierarchy') }}
)

select *
from source