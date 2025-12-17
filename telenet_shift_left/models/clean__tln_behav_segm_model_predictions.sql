with customers as (
    select 
        *
    from {{ source('customer_profiler', 'tln_behav_segm_model_predictions') }}
),

cleaned_customer_number as (
    select
        *,
        CAST(customernumber AS VARCHAR) as cleaned_customernumber
    from customers
),

final as (
    select
        *,
        substr(month_code, 1, 4) || '-' || substr(month_code, 5, 2) as cleaned_month_code
    from cleaned_customer_number
)

select 
    * 
from final