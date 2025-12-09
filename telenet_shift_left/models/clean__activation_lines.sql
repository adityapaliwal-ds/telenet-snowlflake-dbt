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
),

customer_id_mapping as (
    select
        *,
        COALESCE(payer_nc_cust_nbr, payer_cust_nbr) as cleaned_customer_number

    from active_records
)

select 
    *
from customer_id_mapping