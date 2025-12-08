with customers as (
    select 
        *
    from {{ source('customer_profiler', 'tln_behav_segm_model_predictions') }}
),

final as (
    select
        *,
        substr(month_code, 1, 4) || '-' || substr(month_code, 5, 2) as cleaned_month_code
    from customers
)

select 
    * 
from final