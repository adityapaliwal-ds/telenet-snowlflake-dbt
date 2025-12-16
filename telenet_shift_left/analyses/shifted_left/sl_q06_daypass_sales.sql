-------------------------------------------------------------------------------
--Q6 : How many day-passes did we sell within the last three months?
-------------------------------------------------------------------------------

select sum(nr_sales) as total_sales

from {{ ref('clean__sales_products') }}

where
    sales_month between '2025-09' and '2025-11' --specify the period range here
    and lower(product) like '%play sports%pass%' --Play Sports daypass naming patterns
