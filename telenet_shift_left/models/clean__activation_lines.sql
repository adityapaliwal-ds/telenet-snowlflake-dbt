with activation_lines as (
    select
        *
    from {{ source('product_offering_explorer', 'activation_lines') }}
),

active_records as (
    select 
        *
    from activation_lines
    where star_delete_time is null
)

select 
    *
from active_records