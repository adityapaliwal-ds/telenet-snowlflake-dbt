with base as (
    select
        *
    from {{ source('value_reporting_engine', 'active_inst_base_customers_tln') }}
)

, active_records as (
    select
        *

    from base
    where star_delete_time is null
)

, test_customers_exclusion as (
    select
        *

    from active_records
    where lower(cust_cust_cat_group_desc) <> 'tcc'
)

, dtv_types as (
    select
        *
        , case
            when dtv_mix in ('TV Flow', 'YUGO TV') then 'TV Flow'
            when dtv_mix = 'None' then 'Non-DTV'
            else 'TV Iconic'
        end as cleaned_dtv_types
    from test_customers_exclusion
)

, final as (
    select
        *,
        -- make a composite primary key using customer_number and month
        customer_number || '_' || month as cleaned_composite_primary_key
    from dtv_types
)

select
    *
from final
