with source as (
    select
        *
    from {{ source('product_offering_explorer', 'reporting_channel_hierarchy') }}
)

, most_recent as (
    select
        *
    from source
    where most_recent = 1
)

select
    *
from most_recent
