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

telenet_only as (
    select
        *
    from active_records
    where brand = 'Telenet'
),

customer_id_mapping as (
    select
        *,
        COALESCE(payer_nc_cust_nbr, payer_cust_nbr) as cleaned_customer_number

    from telenet_only
)

select 
    *
from customer_id_mapping