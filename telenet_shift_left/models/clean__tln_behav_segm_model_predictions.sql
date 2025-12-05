with customers as (
    select 
        *
    from {{ source('customer_profiler', 'tln_behav_segm_model_predictions') }}
),

final as (
    select
        substr(month_code, 1, 4) || '-' || substr(month_code, 5, 2) as month_code,
        * exclude (month_code)
    from customers
)

select 
    * 
from final