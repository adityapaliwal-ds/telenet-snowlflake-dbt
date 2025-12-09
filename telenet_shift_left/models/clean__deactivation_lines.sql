with deactivation_lines as (
    select
        *
    from {{ source('product_offering_explorer', 'deactivation_lines') }}
),

active_records as (
    select 
        *
    from deactivation_lines
    where star_delete_time is null
)

select 
    *
from active_records