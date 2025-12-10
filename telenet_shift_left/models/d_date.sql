with source as (
    select
        *
    from {{ source('product_offering_explorer', 'd_date') }}
),

exclude_values as (
    select
        *
    from source
    where star_date_id > 0  -- testing values should be excluded
)

select
    *
from exclude_values