----------------------------------------------------------------------------------------
--Q1 : What is the evolution of install base for period x that is not available in Qlik?
----------------------------------------------------------------------------------------

--Q1a : Customer level install base evolution
select 
    month,
    --/*
    --uncomment this if per product install base is needed
    case 
        --Streamz products
        when streamz_basic_volume > 0 then 'Streamz Basic'
        when streamz_volume > 0 then 'Streamz Premium'
        when streamz_plus_volume > 0 then 'Streamz Premium+'
        when play_more_volume > 0 then 'Play More'

        --Play Sports
        when play_sports_volume > 0 then 'Play Sports'

        --Netflix products
        when netflix_basic_volume > 0 then 'Netflix Basic'
        when netflix_standard_volume > 0 then 'Netflix Standard'
        when netflix_premium_volume > 0 then 'Netflix Premium'

        --Disney+ products
        when disney_plus_volume > 0 then 'Disney+ Standard & Premium'
    end as entertainment_product,
    --*/
    count(customer_number) as install_base_customers
from presentation_prod.value_reporting_engine.active_inst_base_customers_tln
where 1=1
    and star_delete_time is null
    --and most_recent_snapshot = '1' --uncomment if only most recent snapshot is needed
    --and month = '2025-10' --specify the month here
    and month between '2023-01' and '2024-12' --specify the period range here
    and --specify the products here
     (
        streamz_basic_volume > 0 --Streamz Basic
        or streamz_volume > 0 --Streamz Premium
        or streamz_plus_volume > 0 --Streamz Premium+
        or play_more_volume > 0 --Play More
        or play_sports_volume > 0 --Play Sports
        or netflix_basic_volume > 0 --Netflix Basic
        or netflix_standard_volume > 0 --Netflix Standard
        or netflix_premium_volume > 0 --Netflix Premium
        or disney_plus_volume > 0 --Disney+ Standard and Disney+ Premium
     )
     --and has_content = 'Y' --if we want all HH with content subscriptions without specific product filter
group by all
order by 1,2
;


--Q1b : Household level install base evolution
select 
    month,
    --/*
    --uncomment this if per product install base is needed
    case 
        --Streamz products
        when streamz_basic_volume > 0 then 'Streamz Basic'
        when streamz_volume > 0 then 'Streamz Premium'
        when streamz_plus_volume > 0 then 'Streamz Premium+'
        when play_more_volume > 0 then 'Play More'

        --Play Sports
        when play_sports_volume > 0 then 'Play Sports'

        --Netflix products
        when netflix_basic_volume > 0 then 'Netflix Basic'
        when netflix_standard_volume > 0 then 'Netflix Standard'
        when netflix_premium_volume > 0 then 'Netflix Premium'

        --Disney+ products
        when disney_plus_volume > 0 then 'Disney+ Standard & Premium'
    end as entertainment_product,
    --*/
    count(distinct star_enr_mix_base_id) as install_base_households --star_enr_mix_base_id is HH ID
from presentation_prod.value_reporting_engine.active_inst_base_households_tln
where 1=1
     and star_delete_time is null
    --and most_recent_snapshot = '1' --uncomment if only most recent snapshot is needed
    --and month = '2025-10' --specify the month here
    and month between '2023-01' and '2024-12' --specify the period range here
    and --specify the products here
     (
        streamz_basic_volume > 0 --Streamz Basic
        or streamz_volume > 0 --Streamz Premium
        or streamz_plus_volume > 0 --Streamz Premium+
        or play_more_volume > 0 --Play More
        or play_sports_volume > 0 --Play Sports
        or netflix_basic_volume > 0 --Netflix Basic
        or netflix_standard_volume > 0 --Netflix Standard
        or netflix_premium_volume > 0 --Netflix Premium
        or disney_plus_volume > 0 --Disney+ Standard and Disney+ Premium
     )
     --and has_content = 'Y' --if we want all HH with content subscriptions without specific product filter
group by all
order by 1,2
;

----------------------------------------------------------------------------
--Q2 : What is the entertainment attach rate of the last month?
----------------------------------------------------------------------------

--All TLN HH with Entertainent content / All TLN HH
select 
    month,
    count(distinct case when has_content = 'Y' then star_enr_mix_base_id end) as hh_with_content,
    count(distinct star_enr_mix_base_id) as total_hh,
    (count(distinct case when has_content = 'Y' then star_enr_mix_base_id end) / nullif(count(distinct star_enr_mix_base_id), 0)) as entertainment_attach_rate
from presentation_prod.value_reporting_engine.active_inst_base_households_tln
where 1=1 
    and star_delete_time is null
    and month between '2025-01' and '2025-11' --specify the period here
group by all
order by 1
;


--All DTV HH with Entertainent content / All DTV HH
select 
    month,
    count(distinct case when has_content = 'Y' and has_dtv = 'Y' then star_enr_mix_base_id end) as hh_with_content,
    count(distinct case when has_dtv = 'Y' then star_enr_mix_base_id end) as total_dtv_hh,

    (count(distinct case when has_content = 'Y' and has_dtv = 'Y' then star_enr_mix_base_id end) --numerator = DTV HH with content
    / 
    nullif(count(distinct case when has_dtv = 'Y' then star_enr_mix_base_id end), 0)) ---denominator = all DTV HH
    as entertainment_attach_rate
from presentation_prod.value_reporting_engine.active_inst_base_households_tln
where 1=1 
    and star_delete_time is null
    and month between '2025-01' and '2025-11' --specify the period here
group by all
order by 1
;


----------------------------------------------------------------------
--Q3 : How many Streamz churn/sales did we have in month x?
----------------------------------------------------------------------

/*********************
--Q3a : Churn version
*********************/

select 
    case 
        when product in ('Streamz Basic',  'Streamz Basic B2B') then 'Streamz Basic'
        when product in ('Streamz Premium',  'Streamz Premium B2B') then 'Streamz Premium'
        when product in ('Streamz Premium+',  'Streamz Premium+ B2B') then 'Streamz Premium+'
        when product in ('Play More',  'Play More B2B') then 'Play More'
        when product in ('Play Sports',  'Play Sports B2B') then 'Play Sports'
        when product in ('Netflix Basic',  'Netflix Basic B2B') then 'Netflix Basic'
        when product in ('Netflix Standard',  'Netflix Standard B2B') then 'Netflix Standard'
        when product in ('Netflix Premium',  'Netflix Premium B2B') then 'Netflix Premium'
        when product in ('Disney+ Standard',  'Disney+ Standard B2B') then 'Disney+ Standard'
        when product in ('Disney+ Premium',  'Disney+ Premium B2B') then 'Disney+ Premium'
    end as entertainment_product,
    -sum(nr_churn) as total_churn --to show positive values for churn
from presentation_prod.product_offering_explorer.churn_products c 
left join presentation_prod.product_offering_explorer.reporting_channel_hierarchy rch 
on c.star_prim_rpt_chnnl_id = rch.star_rpt_channel_hierarchy_id
where 1=1
    and c.star_delete_time is null
    --monthly aggregation
    and churn_month = '2025-10' --specify the month here
    --and churn_month between '2025-01' and '2025-11' --specify the period range here

    --daily aggregation
    --and churn_date = '2025-11-25' --specify the day here
    --and churn_date >= '2025-10-01' --specify the start date here
    --and churn_date <= '2025-11-01' --specify the end date here

    and product in --specify the products here
        (
            'Streamz Basic', 'Streamz Basic B2B',
            'Streamz Premium', 'Streamz Premium B2B',
            'Streamz Premium+', 'Streamz Premium+ B2B',
            'Play More', 'Play More B2B',
            'Play Sports', 'Play Sports B2B',
            'Netflix Basic', 'Netflix Basic B2B',
            'Netflix Standard', 'Netflix Standard B2B',
            'Netflix Premium', 'Netflix Premium B2B',
            'Disney+ Standard', 'Disney+ Standard B2B',
            'Disney+ Premium', 'Disney+ Premium B2B'
        ) 
    and not (--To exclude technical migrations churns that happened in 2025 
        STAR_DATE_ID_CHURN BETWEEN 20250201 AND 20251231 --Migrations happened in 2025 from February to December
        AND REPORTING_CHANNEL_LVL_4_DESC IN ('Telesales', 'Internal')
        AND REPORTING_CHANNEL_LVL_3_DESC = 'Other'
    )
group by all
order by 1
;

/**********************
--Q3b : Sales version
**********************/

select 
    case 
        when product in ('Streamz Basic',  'Streamz Basic B2B') then 'Streamz Basic'
        when product in ('Streamz Premium',  'Streamz Premium B2B') then 'Streamz Premium'
        when product in ('Streamz Premium+',  'Streamz Premium+ B2B') then 'Streamz Premium+'
        when product in ('Play More',  'Play More B2B') then 'Play More'
        when product in ('Play Sports',  'Play Sports B2B') then 'Play Sports'
        when product in ('Netflix Basic',  'Netflix Basic B2B') then 'Netflix Basic'
        when product in ('Netflix Standard',  'Netflix Standard B2B') then 'Netflix Standard'
        when product in ('Netflix Premium',  'Netflix Premium B2B') then 'Netflix Premium'
        when product in ('Disney+ Standard',  'Disney+ Standard B2B') then 'Disney+ Standard'
        when product in ('Disney+ Premium',  'Disney+ Premium B2B') then 'Disney+ Premium'
    end as entertainment_product,
    sum(nr_sales) as total_sales
from presentation_prod.product_offering_explorer.sales_products s
left join presentation_prod.product_offering_explorer.reporting_channel_hierarchy rch 
on s.star_prim_rpt_chnnl_id = rch.star_rpt_channel_hierarchy_id
where 1=1
    and s.star_delete_time is null
    --monthly aggregation
    and sales_month = '2025-10' --specify the month here
    --and sales_month between '2025-01' and '2025-11' --specify the period range here

    --daily aggregation
    --and sales_date = '2025-11-25' --specify the day here
    --and sales_date >= '2025-10-01' --specify the start date here
    --and sales_date <= '2025-11-01' --specify the end date here

    and product in --specify the products here
        (
            'Streamz Basic', 'Streamz Basic B2B',
            'Streamz Premium', 'Streamz Premium B2B',
            'Streamz Premium+', 'Streamz Premium+ B2B',
            'Play More', 'Play More B2B',
            'Play Sports', 'Play Sports B2B',
            'Netflix Basic', 'Netflix Basic B2B',
            'Netflix Standard', 'Netflix Standard B2B',
            'Netflix Premium', 'Netflix Premium B2B',
            'Disney+ Standard', 'Disney+ Standard B2B',
            'Disney+ Premium', 'Disney+ Premium B2B'
        ) 
    and not (--To exclude technical migrations sales that happened in 2025 
        STAR_DATE_ID BETWEEN 20250201 AND 20251231 --Migrations happened in 2025 from February to December
        AND REPORTING_CHANNEL_LVL_4_DESC IN ('Telesales', 'Internal')
        AND REPORTING_CHANNEL_LVL_3_DESC = 'Other'
    )  
group by all
order by 1
;

----------------------------------------------------------------------------------------
--Q4 : What is the number of Play Sports' churn per sales channel in the last 13 months?
----------------------------------------------------------------------------------------

--NB : 
--1. Always use the the reporting sales/churns channels fields in presentation_prod.product_offering_explorer.reporting_channel_hierarchy
--2. Recommended to use levels 1 only because levels 1 to 4 are too much detailed for business. Provide other levels only upon request.

select 
    churn_month,
    product, -- use this to split B2B vs RES
    /* --Use this to group RES & B2B together
    case 
        when product in ('Streamz Basic',  'Streamz Basic B2B') then 'Streamz Basic'
        when product in ('Streamz Premium',  'Streamz Premium B2B') then 'Streamz Premium'
        when product in ('Streamz Premium+',  'Streamz Premium+ B2B') then 'Streamz Premium+'
        when product in ('Play More',  'Play More B2B') then 'Play More'
        when product in ('Play Sports',  'Play Sports B2B') then 'Play Sports'
        when product in ('Netflix Basic',  'Netflix Basic B2B') then 'Netflix Basic'
        when product in ('Netflix Standard',  'Netflix Standard B2B') then 'Netflix Standard'
        when product in ('Netflix Premium',  'Netflix Premium B2B') then 'Netflix Premium'
        when product in ('Disney+ Standard',  'Disney+ Standard B2B') then 'Disney+ Standard'
        when product in ('Disney+ Premium',  'Disney+ Premium B2B') then 'Disney+ Premium'
    end as entertainment_product,
    */
    rch.reporting_channel_lvl_1_desc as sales_channel,
    -sum(nr_churn) as total_churn --to show positive values for churn
from presentation_prod.product_offering_explorer.churn_products c 
left join presentation_prod.product_offering_explorer.reporting_channel_hierarchy rch 
on c.star_prim_rpt_chnnl_id = rch.star_rpt_channel_hierarchy_id
where 1=1
    and c.star_delete_time is null
    --monthly aggregation
    --and churn_month = '2025-10' --specify the month here
    and churn_month between '2024-10' and '2025-10' --specify the period range here

    --daily aggregation
    --and churn_date = '2025-11-25' --specify the day here
    --and churn_date >= '2025-10-01' --specify the start date here
    --and churn_date <= '2025-11-01' --specify the end date here


    and product in ('Play Sports',  'Play Sports B2B') --to filter on a specific product using below products mapping
    /*
    and product in --specify the products here
        (
            'Streamz Basic', 'Streamz Basic B2B',
            'Streamz Premium', 'Streamz Premium B2B',
            'Streamz Premium+', 'Streamz Premium+ B2B',
            'Play More', 'Play More B2B',
            'Play Sports', 'Play Sports B2B',
            'Netflix Basic', 'Netflix Basic B2B',
            'Netflix Standard', 'Netflix Standard B2B',
            'Netflix Premium', 'Netflix Premium B2B',
            'Disney+ Standard', 'Disney+ Standard B2B',
            'Disney+ Premium', 'Disney+ Premium B2B'
        ) 
    */
    and not (--To exclude technical migrations churns that happened in 2025 
        STAR_DATE_ID_CHURN BETWEEN 20250201 AND 20251231 --Migrations happened in 2025 from February to December
        AND REPORTING_CHANNEL_LVL_4_DESC IN ('Telesales', 'Internal')
        AND REPORTING_CHANNEL_LVL_3_DESC = 'Other'
    )
group by all
order by 1, total_churn desc
;

----------------------------------------------------------------------------
--Q5 : 
-- a. How many "entertainment" products on average per household/customer?
-- b. How many customers/households have more than 1/2/3 entertainment products?
----------------------------------------------------------------------------

/********************** Household level insights *************************/

--Q5a : How many "entertainment" products on average per household?
select
    month,
    sum(coalesce(array_size(split(cont_mix, '+')), 0) * nbr_hh_eop) --numerator : total number of entertainment products across all households
    / 
    nullif(sum(nbr_hh_eop), 0) as avg_products_per_hh --denominator : total number of households
from presentation_prod.value_reporting_engine.active_inst_base_households_tln
where 1=1
  and star_delete_time is null
  and month = '2025-10'
  and has_content = 'Y' --considering only households with at least 1 entertainment product
group by month
;


--Q5b : How many households have more than 1/2/3 entertainment products?
select 
    month,
    -- Count the number of products by splitting cont_mix on '+' delimiter
    array_size(split(cont_mix, '+')) as nbr_products_per_hh,
    sum(nbr_hh_eop) as nbr_households
from presentation_prod.value_reporting_engine.active_inst_base_households_tln
where 1=1 
    and star_delete_time is null
    and month = '2025-10' --specify the month here
    and has_content = 'Y'
group by 1,2
order by 3 desc
;

/********************** Customer level insights *************************/

--Q5a : How many "entertainment" products on average per customer?
select
    month,
    sum(coalesce(array_size(split(cont_mix, '+')), 0))--numerator : total entertainment products across all customers
    / 
    nullif(count(distinct customer_number), 0) as avg_products_per_customer --denominator : total number of customers
from presentation_prod.value_reporting_engine.active_inst_base_customers_tln
where star_delete_time is null
  and month = '2025-10'
  and has_content = 'Y'
group by month;


--Q5b : How many customers have more than 1/2/3 entertainment products?
select 
    month,
    -- Count the number of products by splitting cont_mix on '+' delimiter
    array_size(split(cont_mix, '+')) as nbr_products_per_customer,
    count(distinct customer_number) as nbr_customers
from presentation_prod.value_reporting_engine.active_inst_base_customers_tln
where 1=1
    and star_delete_time is null
    and month = '2025-10' --specify the month here
    and has_content = 'Y'
group by 1,2
order by 3 desc
;


-------------------------------------------------------------------------------
--Q6 : How many day-passes did we sell within the last three months?
-------------------------------------------------------------------------------

select 
    'Play Sports DayPasses' as product,
    sales_month,
    sum(nr_sales) as total_sales
from presentation_prod.product_offering_explorer.sales_products s
left join presentation_prod.product_offering_explorer.reporting_channel_hierarchy rch 
on s.star_prim_rpt_chnnl_id = rch.star_rpt_channel_hierarchy_id
where 1=1
    and s.star_delete_time is null
    --monthly aggregation
    --and sales_month = '2025-10' --specify the month here
    and sales_month between '2025-08' and '2025-10' --specify the period range here

    --daily aggregation
    --and sales_date = '2025-11-25' --specify the day here
    --and sales_date >= '2025-10-01' --specify the start date here
    --and sales_date <= '2025-11-01' --specify the end date here

    --weekly and yearly are also possible using respectively sales_week and sales_year fields

    and lower(product) like '%play sports%pass%' --Play Sports daypass naming patterns
group by all
order by 2
;


----------------------------------------------------------------------------------------
--Q7 : What is the share of TV shop's sales in digital sales in the last x months?
----------------------------------------------------------------------------------------
select 
    sales_month,
    /*
    case 
        when product in ('Streamz Basic',  'Streamz Basic B2B') then 'Streamz Basic'
        when product in ('Streamz Premium',  'Streamz Premium B2B') then 'Streamz Premium'
        when product in ('Streamz Premium+',  'Streamz Premium+ B2B') then 'Streamz Premium+'
        when product in ('Play More',  'Play More B2B') then 'Play More'
        when product in ('Play Sports',  'Play Sports B2B') then 'Play Sports'
        when product in ('Netflix Basic',  'Netflix Basic B2B') then 'Netflix Basic'
        when product in ('Netflix Standard',  'Netflix Standard B2B') then 'Netflix Standard'
        when product in ('Netflix Premium',  'Netflix Premium B2B') then 'Netflix Premium'
        when product in ('Disney+ Standard',  'Disney+ Standard B2B') then 'Disney+ Standard'
        when product in ('Disney+ Premium',  'Disney+ Premium B2B') then 'Disney+ Premium'
    end as entertainment_product,
    */
    case 
        when lower(rch.reporting_channel_lvl_4_desc) like '%tv shop%' then 'Digital - TV Shop'
        else 'Digital Web or App'
    end as digital_channel_split,
    sum(nr_sales) as total_sales,

    -- share of sales per digital_channel_split over total digital sales
    to_char(
        (sum(nr_sales) / nullif(sum(sum(nr_sales)) over (), 0)) * 100,
        'FM999990.00'
    ) || '%' as shares_per_channel
    
from presentation_prod.product_offering_explorer.sales_products s
left join presentation_prod.product_offering_explorer.reporting_channel_hierarchy rch 
on s.star_prim_rpt_chnnl_id = rch.star_rpt_channel_hierarchy_id
where 1=1
    and s.star_delete_time is null
    --monthly aggregation
    and sales_month = '2025-10' --specify the month here
    --and sales_month between '2025-01' and '2025-11' --specify the period range here

    --daily aggregation
    --and sales_date = '2025-11-25' --specify the day here
    --and sales_date >= '2025-10-01' --specify the start date here
    --and sales_date <= '2025-11-01' --specify the end date here

    and product in ('Streamz Basic',  'Streamz Basic B2B') --to filter on a specific product using below products mapping
    /*
    and product in --specify the products here
        (
            'Streamz Basic', 'Streamz Basic B2B',
            'Streamz Premium', 'Streamz Premium B2B',
            'Streamz Premium+', 'Streamz Premium+ B2B',
            'Play More', 'Play More B2B',
            'Play Sports', 'Play Sports B2B',
            'Netflix Basic', 'Netflix Basic B2B',
            'Netflix Standard', 'Netflix Standard B2B',
            'Netflix Premium', 'Netflix Premium B2B',
            'Disney+ Standard', 'Disney+ Standard B2B',
            'Disney+ Premium', 'Disney+ Premium B2B'
        ) 
    */
    and rch.reporting_channel_lvl_1_desc = 'Digital' --to filter only digital sales channels 
    and not (--To exclude technical migrations sales that happened in 2025 
        STAR_DATE_ID BETWEEN 20250201 AND 20251231 --Migrations happened in 2025 from February to December
        AND REPORTING_CHANNEL_LVL_4_DESC IN ('Telesales', 'Internal')
        AND REPORTING_CHANNEL_LVL_3_DESC = 'Other'
    )
    
group by all
order by 1
;


/*******************************************************REMAINING GREEN QUESTIONS**********************************************************/

--------------------------------------------------------------------------------------------------------------------
--Q8 : What is the profile of our non-DTV customers? (Product holding, product subscriptions, socio-demographics)
--------------------------------------------------------------------------------------------------------------------

--a. Lifestage split (model should ask which level of details is needed)
select
    month,
    l.lifestage, --grouped 
    --l.lifestage_details, --detailed
    count(distinct customer_number) as nbr_customers
from presentation_prod.value_reporting_engine.active_inst_base_customers_tln c 
left join presentation_prod.customer_profiler.lifestage_model_predictions l
on cast(c.customer_number as number) = l.customernumber
and to_number(replace(c.month, '-', '') )= l.month_code
where 1=1
  and c.star_delete_time is null
  and c.month = '2025-10'--Important note is that the dataset is refreshed monthly before 15th of each month for the previous month
  and c.has_content = 'Y'
  and c.has_dtv = 'N' --non-DTV customers
group by 1, 2
order by 1, 2
;


--B. Product subscriptions
select
    month,
    case 
        when streamz_basic_volume > 0 then 'Streamz Basic'
        when streamz_volume > 0 then 'Streamz Premium'
        when streamz_plus_volume > 0 then 'Streamz Premium+'
        when play_more_volume > 0 then 'Play More'
        when play_sports_volume > 0 then 'Play Sports'
        when netflix_basic_volume > 0 then 'Netflix Basic'
        when netflix_standard_volume > 0 then 'Netflix Standard'
        when netflix_premium_volume > 0 then 'Netflix Premium'
        when disney_plus_volume > 0 then 'Disney+ Standard & Premium'
    end as product,
    count(distinct customer_number) as nbr_customers
from presentation_prod.value_reporting_engine.active_inst_base_customers_tln
where 1=1
  and star_delete_time is null
  and month = '2025-11'
  and has_content = 'Y'
  and has_dtv = 'N' --non-DTV customers
group by 1, 2
order by 1, 2
;

--c. Behavioral segmentation
select
    month,
    res.predictions as res_behavior_segment,
    count(distinct customer_number) as nbr_customers
from presentation_prod.value_reporting_engine.active_inst_base_customers_tln c
left join presentation_prod.customer_profiler.tln_behav_segm_model_predictions res
on cast(c.customer_number as number) = res.customernumber
and to_number(replace(c.month, '-', '') )= res.month_code
where 1=1
  and c.star_delete_time is null
  and c.month = '2025-11'--Important note is that the dataset is refreshed monthly before 15th of each month for the previous month
  and c.has_content = 'Y'
  and c.has_dtv = 'N' --non-DTV customers
group by 1, 2
order by 1, 2
;


-------------------------------------------------------------------------------------------------------------------
--Q9 : How many e.g. Play Sports churners also churned their DTV line in month x? (with a join between 2 datasets)
-------------------------------------------------------------------------------------------------------------------

select
    churn_month,
    case
        when rgu_mix_before_churn like '%ETV%' and rgu_mix_after_churn not like '%ETV%' then 'Churned DTV & Play Sports'
        else 'Play Sports Only Churners'
    end as churn_type,
    count(coalesce(payer_nc_cust_nbr,payer_cust_nbr)) as nbr_churners
from presentation_prod.product_offering_explorer.churn_products c 
left join presentation_prod.product_offering_explorer.reporting_channel_hierarchy rch 
on c.star_prim_rpt_chnnl_id = rch.star_rpt_channel_hierarchy_id
where 1=1
    and c.star_delete_time is null
    --monthly aggregation
    and churn_month = '2025-11' --specify the month here
    --and churn_month between '2025-01' and '2025-11' --specify the period range here
    and product in ('Play Sports',  'Play Sports B2B') --to filter on a specific product using below products mapping
    and not (--To exclude technical migrations churns that happened in 2025 
        STAR_DATE_ID_CHURN BETWEEN 20250201 AND 20251231 --Migrations happened in 2025 from February to December
        AND REPORTING_CHANNEL_LVL_4_DESC IN ('Telesales', 'Internal')
        AND REPORTING_CHANNEL_LVL_3_DESC = 'Other'
    )
group by 1,2
order by 1,2
;

--------------------------------------------------------------------------------
--Q10 : How many customers have 5% discount?  (insight available in one dataset)
--------------------------------------------------------------------------------
--Rule : As of Marketplace in 2025, customers with at least 2 entertainment products in their content mix 
--get 5% discount on their total entertainment spend.
Select 
    month,
    cont_mix,--for verification purpose
    count(distinct customer_number) as nbr_customers_with_5pct_discount
from presentation_prod.value_reporting_engine.active_inst_base_customers_tln
where 1=1
  and star_delete_time is null
  and most_recent_snapshot = '1'--to provide most recent snapshot only
  --and month = '2025-10'--specify the month here for a month specific insight
  and has_content = 'Y'
  and cont_mix like '%+%' --to filter customers with at least 2 entertainment products in content mix
group by 1, 2
order by 1, 2
;

----------------------------------------------------------------------------------------------------
--Q11 : How many cancellations did we have in the last x months? (insight available in one dataset)
----------------------------------------------------------------------------------------------------
--Ask business to refine what cancellations they mean (entertainment product cancellations or overall cancellations including internet, DTV, mobile, fixed line, etc.)

--For lines level cancellations : MORE RELEVANT FOR CONNECTIVITY SCOPE
select 
    cancel_month,
    line_product, --use this column to filter on any line product
    sum(nr_cancels) as total_cancellations
from presentation_prod.product_offering_explorer.cancel_lines
where 1=1
    and star_delete_time is null
    and cancel_month = '2025-11' --specify the month here
    --and cancel_month between '2025-01' and '2025-10' --specify the period range here
group by 1, 2
order by 1, 2
;

-- For entertainment product cancellations : MORE RELEVANT FOR ENT SCOPE
select 
    d.month as cancel_month,
    case 
        when pp.product_desc in ('Streamz Basic',  'Streamz Basic B2B') then 'Streamz Basic'
        when pp.product_desc in ('Streamz Premium',  'Streamz Premium B2B') then 'Streamz Premium'
        when pp.product_desc in ('Streamz Premium+',  'Streamz Premium+ B2B') then 'Streamz Premium+'
        when pp.product_desc in ('Play More',  'Play More B2B') then 'Play More'
        when pp.product_desc in ('Play Sports',  'Play Sports B2B') then 'Play Sports'
        when pp.product_desc in ('Netflix Basic',  'Netflix Basic B2B') then 'Netflix Basic'
        when pp.product_desc in ('Netflix Standard',  'Netflix Standard B2B') then 'Netflix Standard'
        when pp.product_desc in ('Netflix Premium',  'Netflix Premium B2B') then 'Netflix Premium'
        when pp.product_desc in ('Disney+ Standard',  'Disney+ Standard B2B') then 'Disney+ Standard'
        when pp.product_desc in ('Disney+ Premium',  'Disney+ Premium B2B') then 'Disney+ Premium'
    end as entertainment_product,
    sum(nap.REG_ORG_CNCL_NET_ADD_PRD_NO) as total_cancellations
from PRESENTATION_PROD.PRODUCT_OFFERING_EXPLORER.REGISTERED_NET_ADDS_PRODUCTS nap 
left join PRESENTATION_PROD.PRODUCT_OFFERING_EXPLORER.REPORTING_CHANNEL_HIERARCHY rch on nap.STAR_PRIM_RPT_CHNNL_ID = rch.STAR_RPT_CHANNEL_HIERARCHY_ID
left join PRESENTATION_PROD.PRODUCT_OFFERING_EXPLORER.D_MTS_ENROLLMENT_PRODUCT pp on nap.STAR_PRODUCT_ID=pp.star_enrollment_product_id
left outer join PRESENTATION_PROD.PRODUCT_OFFERING_EXPLORER.D_DATE d on nap.STAR_TRANSACTION_DATE_ID=d.STAR_DATE_ID
where 1=1
    and nap.star_delete_time is null
    and nap.star_transaction_date_id between 20251101 and 20251130 --specify the month here
    and pp.product_desc in --specify the products here
        (
            'Streamz Basic', 'Streamz Basic B2B',
            'Streamz Premium', 'Streamz Premium B2B',
            'Streamz Premium+', 'Streamz Premium+ B2B',
            'Play More', 'Play More B2B',
            'Play Sports', 'Play Sports B2B',
            'Netflix Basic', 'Netflix Basic B2B',
            'Netflix Standard', 'Netflix Standard B2B',
            'Netflix Premium', 'Netflix Premium B2B',
            'Disney+ Standard', 'Disney+ Standard B2B',
            'Disney+ Premium', 'Disney+ Premium B2B'
        ) 
    
    and not (--To exclude technical migrations churns that happened in 2025 
        nap.star_transaction_date_id BETWEEN 20250201 AND 20251231 --Migrations happened in 2025 from February to December
        AND rch.reporting_channel_lvl_4_desc IN ('Telesales', 'Internal')
        AND rch.reporting_channel_lvl_3_desc = 'Other'
    )
    
group by all
order by 1, 2
;



------------------------------------------------------------------------------------------------
--Q12 : How many CAM CI+ customers/devices do we still have? (insight available in one dataset)
------------------------------------------------------------------------------------------------
SELECT 
        star_snapshot_month_id as snapshot_month,
        CASE 
            WHEN DEVICE_PRODUCT_TYPE IN ('2008C-STB-TN', 'DCX960') THEN 'EOS'
            WHEN DEVICE_PRODUCT_TYPE IN ('CAM1300', 'CI0500-TNO01-31') THEN 'CAM CI+'
            WHEN DEVICE_PRODUCT_TYPE = 'DTVBC_APPEARTV' THEN 'DTV Broadcast Center' --TV Screens in TLN Headquarters used for tests and/or demos
            WHEN DEVICE_PRODUCT_TYPE LIKE 'AD%' THEN 'SIPADAN'
            WHEN DEVICE_PRODUCT_TYPE = 'VIP7002W' THEN 'APOLLO'
        END AS DEVICE_CATEGORIES,
        count(distinct customer_number) AS nbr_customers, -- Number of customers with CAM CI+ devices using most recent snapshot
        count(distinct device_serial_val) AS nbr_devices -- Number of CAM CI+ devices using most recent snapshot
    FROM presentation_prod.product_offering_explorer.active_inst_base_stb_month
    WHERE 1=1
        AND star_delete_time IS NULL
        and most_recent_snapshot = '1' --to provide most recent snapshot only
        and device_product_type in ('CAM1300', 'CI0500-TNO01-31') --specify the device type here
    GROUP BY 1, 2
    ORDER BY 1, 2
;

/*******************************************************YELLOW QUESTIONS**********************************************************/

--------------------------------------------------------------------------------------------------------
--Q13 : How many existing e.g. Streamz clients subscribed to promo x?  (join between multiple datasets) --> becomes RED question!
--------------------------------------------------------------------------------------------------------

--To answer to this question, we need to scope : 
--1. The 'existing' with business : is it existing with Streamz 
--2. The exact promo 'x' : which most of the time requires a mapping table between promo technical names in the datasets and promo shorten names as used by business 

--To avoid the agent to hallucinate due to complexity and mapping needed for promos, we would recommend to exclude such questions from the scope the model can handle and redirect to analysts instead.
--Qlik dashboards with accurate logic can be used for such complex questions.



-------------------------------------------------------------------------------------------------------------------------------------------
--Q14 : How many e.g. Streamz subscribers kept their subscription at the end of e.g. the one-month promo?  (join between multiple datasets) becomes RED question!
-------------------------------------------------------------------------------------------------------------------------------------------

--This becomes a RED question due to complexity and mapping needed for promos.
--Promo related questions should be redirected to analysts due to complexity and mapping needed for promos.

------------------------------------------------------------------------------------------------------------------------------
--Q15: Amongst new internet lines since e.g. September, how many subscribed to e.g. Play Sports within the following 2 months? 
--Q15 b : How many subscribed through a promo? (join between multiple datasets) --> becomes RED question and should be redirected to analysts!
------------------------------------------------------------------------------------------------------------------------------
-- 1) Define the months in scope (e.g. from 2025-09 onwards)
WITH months AS (
    SELECT '2025-09' AS month UNION ALL
    SELECT '2025-10' UNION ALL
    SELECT '2025-11' UNION ALL
    SELECT '2025-12'
),

-- 2) New Internet activations only (no Int before, Int after)
new_internet_activations AS (
    SELECT 
        activation_month AS internet_activation_month,
        COALESCE(payer_nc_cust_nbr, payer_cust_nbr) AS customer_number
    FROM presentation_prod.product_offering_explorer.activation_lines
    WHERE 1=1
      AND star_delete_time IS NULL
      AND activation_month BETWEEN '2025-09' AND '2025-12'  -- adjust range here
      AND rgu_mix_before_activation NOT LIKE '%Int%'        -- no Internet before
      AND rgu_mix_after_activation LIKE '%Int%'             -- Internet after
),

-- 3) Play Sports base (customers with Play Sports active)
play_sports_base AS (
    SELECT 
        month,
        customer_number,
        cont_mix
    FROM presentation_prod.value_reporting_engine.active_inst_base_customers_tln
    WHERE 1=1
      AND star_delete_time IS NULL
      AND month BETWEEN '2025-09' AND '2026-02'  -- need to cover activation + 2 months window
      AND (
            play_sports_volume > 0      -- adapt to correct field name(s) for Play Sports
      )
),

-- 4) First Play Sports subscription within 2 months after new Internet activation
play_sports_within_2m AS (
    SELECT 
        a.customer_number,
        a.internet_activation_month,
        MIN(p.month) AS first_play_sports_month
    FROM new_internet_activations a
    JOIN play_sports_base p
      ON p.customer_number = a.customer_number
     -- Play Sports must start between activation month and +2 months
     AND p.month BETWEEN 
            a.internet_activation_month 
        AND TO_CHAR(
                ADD_MONTHS(TO_DATE(a.internet_activation_month, 'YYYY-MM'), 2),
                'YYYY-MM'
            )  -- 2-month window (activation month + 2)
    GROUP BY a.customer_number, a.internet_activation_month
)

-- 5) Final: per activation month, how many new Internet lines and how many took Play Sports within 2 months?
SELECT 
    m.month,
    COUNT(DISTINCT a.customer_number) AS new_internet_customers,
    COUNT(DISTINCT ps.customer_number) AS playsports_within_2m
FROM months m
LEFT JOIN new_internet_activations a 
       ON a.internet_activation_month = m.month
LEFT JOIN play_sports_within_2m ps 
       ON ps.internet_activation_month = m.month
GROUP BY m.month
ORDER BY m.month;


----------------------------------------------------------------------------------------------------
--Q16 : How many new Internet and/or DTV customers take Streamz immediately? 
--And within the 12 months after taking Streamz, also take Netflix? (join between multiple datasets)
----------------------------------------------------------------------------------------------------
--NB : 'immediately' is defined here as within the first 3 months after new Internet and/or DTV activation.
--NB : 'within the 12 months after taking Streamz' is defined here as within the first 9 months after Streamz subscription (to keep overall timeframe within 1 year from activation).
--NB : Here I limit the insights to 2025 months for testing purpose, but we can go back to earlier years if needed or further limit the months range based on business inputs.

with months as (
    select '2025-01' as month union all
    select '2025-02' union all
    select '2025-03' union all
    select '2025-04' union all
    select '2025-05' union all
    select '2025-06' union all
    select '2025-07' union all
    select '2025-08' union all
    select '2025-09' union all
    select '2025-10' union all
    select '2025-11' union all
    select '2025-12'
),

new_internet_dtv_activations as (
    select 
        activation_month as int_and_or_dtv_activation_month,
        coalesce(payer_nc_cust_nbr, payer_cust_nbr) as customer_number
    from presentation_prod.product_offering_explorer.activation_lines
    where 1=1
      and star_delete_time is null
      and activation_month between '2025-01' and '2025-12' -- new internet and/or DTV activations in 2025 (months can be adjusted based on business needs)
      and (
            rgu_mix_before_activation not like '%Int%'
         and rgu_mix_before_activation not like '%ETV%'
      )-- did not have Internet and/or DTV customers before
      and (
            rgu_mix_after_activation like '%Int%'
         or rgu_mix_after_activation like '%ETV%'
      )-- new Internet and/or DTV customers after
),

streamz_base as (
    select 
        month,
        customer_number,
        cont_mix
    from presentation_prod.value_reporting_engine.active_inst_base_customers_tln
    where 1=1
      and star_delete_time is null
      and month >= '2025-01' and month <= '2025-12'-- considering Streamz base within 2025 (months can be adjusted based on business needs)
      and (
            streamz_basic_volume > 0 
         or streamz_volume > 0
         or streamz_plus_volume > 0
      )
),

netflix_base as (
    select 
        month,
        customer_number,
        cont_mix
    from presentation_prod.value_reporting_engine.active_inst_base_customers_tln
    where 1=1
      and star_delete_time is null
    and month >= '2025-01' and month <= '2025-12' -- considering Netflix base within 2025 (months can be adjusted based on business needs)
      and (
            netflix_basic_volume > 0 
         or netflix_standard_volume > 0
         or netflix_premium_volume > 0
      )
),

-- Step 1: first Streamz within 3M after activation
streamz_within_3m as (
    select 
        a.customer_number,
        a.int_and_or_dtv_activation_month,
        min(s.month) as first_streamz_month
    from new_internet_dtv_activations a
    join streamz_base s
      on s.customer_number = a.customer_number
     and s.cont_mix like '%Streamz%'
     and s.cont_mix not like '%Netflix%'
     and s.month between a.int_and_or_dtv_activation_month and to_char(add_months(to_date(a.int_and_or_dtv_activation_month, 'YYYY-MM'), 2), 'YYYY-MM')-- 3 months window
    group by a.customer_number, a.int_and_or_dtv_activation_month
),

-- Step 2: Netflix + Streamz within 9M after Streamz
netflix_on_top as (
    select 
        s.customer_number,
        s.int_and_or_dtv_activation_month,
        s.first_streamz_month,
        min(n.month) as netflix_added_month
    from streamz_within_3m s
    join netflix_base n
      on n.customer_number = s.customer_number
     and n.cont_mix like '%Netflix%'
     and n.cont_mix like '%Streamz%'
     and n.month between s.first_streamz_month and to_char(add_months(to_date(s.first_streamz_month, 'YYYY-MM'), 8), 'YYYY-MM') -- 9 months window
    group by s.customer_number, s.int_and_or_dtv_activation_month, s.first_streamz_month
)

select 
    m.month,
    count(distinct a.customer_number) as new_int_or_dtv_or_intplusdtv_customers,
    count(distinct s.customer_number) as streamz_within_3m,
    count(distinct n.customer_number) as netflix_on_top_within_9m
from months m
left join new_internet_dtv_activations a on a.int_and_or_dtv_activation_month = m.month
left join streamz_within_3m s on s.int_and_or_dtv_activation_month = m.month
left join netflix_on_top n on n.netflix_added_month = m.month
group by m.month
order by m.month
;

-------------------------------------------------------------------------------------------------------------
-- Q17 : How many new internet customers take Streamz in the first 3 months of their clientship 
-- and subsequently add Netflix to their products within 6 months as of first_streamz_month?
-------------------------------------------------------------------------------------------------------------
-- Business logic :
-- 1) NEW INTERNET customers: no Int before, Int after.
-- 2) They take Streamz within 3 months after Internet activation.
-- 3) Then they add Netflix ON TOP OF Streamz within 6 months starting from first_streamz_month:
--      window = [first_streamz_month + 1, first_streamz_month + 5].
-- 4) Aggregate by Internet activation month.

--NB : the query took more than 40min to run in Snowflake. To allow test for the POC, I suggest to replace the question by the below question.
-------------------------------------------------------------------------------------------------------------

/*
with

-- 0) Months dimension (2025 only; you can extend rowcount & start date if needed)
months as (
    select
        date_trunc('month', dateadd(month, seq4(), to_date('2025-01-01'))) as month_date
    from table(generator(rowcount => 12))
),

-- 1) New INTERNET activations (start of clientship)
new_internet_activations as (
    select
        -- assume activation_month is 'YYYY-MM' -> convert to first day of month
        to_date(activation_month || '-01', 'YYYY-MM-DD') as internet_activation_date,
        coalesce(payer_nc_cust_nbr, payer_cust_nbr)      as customer_number
    from presentation_prod.product_offering_explorer.activation_lines
    where star_delete_time is null
      and activation_month between '2025-01' and '2025-12'         -- restrict activations to 2025
      and rgu_mix_before_activation not like '%Int%'               -- no Internet before
      and rgu_mix_after_activation  like '%Int%'                   -- Internet after
),

-- 2) Single scan of the base customers table
base as (
    select
        to_date(month || '-01', 'YYYY-MM-DD') as month_date,       -- first day of that month
        customer_number,
        cont_mix,
        -- boolean flags to avoid re-computing volume logic
        (streamz_basic_volume > 0 or streamz_volume > 0 or streamz_plus_volume > 0) as has_streamz,
        (netflix_basic_volume > 0 or netflix_standard_volume > 0 or netflix_premium_volume > 0) as has_netflix
    from presentation_prod.value_reporting_engine.active_inst_base_customers_tln
    where star_delete_time is null
      -- IMPORTANT: if you want full 3M + 6M windows for late-2025 activations,
      -- we should extend this range into 2026 (e.g. '2025-01' to '2026-12'). But keeping it until end of 2025 for faster running time
      and month between '2025-01' and '2025-12'
),

streamz_base as (
    select month_date, customer_number, cont_mix
    from base
    where has_streamz
),

netflix_base as (
    select month_date, customer_number, cont_mix
    from base
    where has_netflix
),

-- 3) First Streamz month within 3 months after Internet activation
streamz_within_3m as (
    select
        a.customer_number,
        a.internet_activation_date,
        s.month_date as first_streamz_date
    from new_internet_activations a
    join streamz_base s
      on s.customer_number = a.customer_number
     -- window: activation_month .. activation_month + 2 (3 months)
     and s.month_date between a.internet_activation_date
                           and dateadd(month, 2, a.internet_activation_date)
    -- pick the earliest Streamz month in that window per customer + activation date
    qualify row_number() over (
        partition by a.customer_number, a.internet_activation_date
        order by s.month_date
    ) = 1
),

-- 4) First Netflix month ON TOP OF Streamz within 6 months as of first_streamz_date
netflix_subsequently_within_6m as (
    select
        s.customer_number,
        s.internet_activation_date,
        s.first_streamz_date,
        n.month_date as netflix_added_date
    from streamz_within_3m s
    join netflix_base n
      on n.customer_number = s.customer_number
     -- Netflix month must have both Netflix AND Streamz in cont_mix
     -- (strict interpretation of "Netflix on top of Streamz")
     and n.cont_mix like '%Netflix%'
     and n.cont_mix like '%Streamz%'
     -- window: [first_streamz_date + 1 month, first_streamz_date + 5 months]
     and n.month_date between dateadd(month, 1, s.first_streamz_date)
                         and dateadd(month, 5, s.first_streamz_date)
    qualify row_number() over (
        partition by s.customer_number, s.internet_activation_date, s.first_streamz_date
        order by n.month_date
    ) = 1          -- first qualifying NF month in the 6M window
)

-- 5) Final aggregation by Internet activation month (YYYY-MM)
select
    to_char(m.month_date, 'YYYY-MM') as internet_activation_month,
    count(distinct a.customer_number) as new_internet_customers,
    count(distinct s.customer_number) as streamz_within_3m,
    count(distinct n.customer_number) as streamz_then_netflix_within_6m_from_streamz
from months m
left join new_internet_activations a
       on date_trunc('month', a.internet_activation_date) = m.month_date
left join streamz_within_3m s
       on date_trunc('month', s.internet_activation_date) = m.month_date
left join netflix_subsequently_within_6m n
       on date_trunc('month', n.internet_activation_date) = m.month_date
group by m.month_date
order by m.month_date
;
*/


-- ========================================================================================================================
-- Q17 - Replacement : How many customers already having Streamz in previous month take Netflix on top in current month and end-up in the Netflix-Streamz combo?
-- Technical translation : How many Streamz customers have transitioned from standalone STR products in M-1 to a combination of STR and NF products in M?
-- ========================================================================================================================

--3. Full period analysis from January to December 2025 - monthly aggregation
with months as (
    -- Define the months to analyze
    select '2024-12' as month union all
    select '2025-01' union all
    select '2025-02' union all
    select '2025-03' union all
    select '2025-04' union all
    select '2025-05' union all
    select '2025-06' union all
    select '2025-07' union all
    select '2025-08' union all
    select '2025-09' union all
    select '2025-10' union all
    select '2025-11' union all
    select '2025-12'
),

streamz_base as (
    -- Monthly view of customers who have Streamz products (any tier)
    select 
        month,
        customer_number,
        cont_mix
    from presentation_prod.value_reporting_engine.active_inst_base_customers_tln
    where 1=1
      and star_delete_time is null
      and (
            streamz_basic_volume > 0 
         or streamz_volume > 0
         or streamz_plus_volume > 0
      )
),

netflix_base as (
    -- Monthly view of customers who have Netflix products (any tier)
    select 
        month,
        customer_number,
        cont_mix
    from presentation_prod.value_reporting_engine.active_inst_base_customers_tln
    where 1=1
      and star_delete_time is null
      and (
            netflix_basic_volume > 0 
         or netflix_standard_volume > 0
         or netflix_premium_volume > 0
      )
),

month_pairs as (
    -- Generate consecutive month pairs (previous month → current month)
    select 
        m1.month as prev_month,
        m2.month as curr_month
    from months m1
    join months m2
      on date_trunc('month', to_date(m1.month || '-01', 'YYYY-MM-DD') + interval '1 month') 
         = date_trunc('month', to_date(m2.month || '-01', 'YYYY-MM-DD'))
),

joined as (
    -- Join Streamz base (month m) with Netflix base (month m+1)
    select 
        mp.prev_month,
        mp.curr_month,
        s.customer_number,
        s.cont_mix as cont_mix_prev,
        n.cont_mix as cont_mix_curr
    from month_pairs mp
    join streamz_base s 
      on s.month = mp.prev_month
    left join netflix_base n 
      on n.customer_number = s.customer_number
     and n.month = mp.curr_month
    where 1=1
      -- Keep only Streamz-only customers in month m
      and (
            (s.cont_mix in ('Streamz', 'Streamz Basic', 'Streamz Plus')) --standalone STR
            or (s.cont_mix not like '%Netflix%') --STR mixed with other products than Netflix
          )
)

-- Aggregate: count total Streamz-only and those who added NF in the next month
select 
    j.prev_month as month,
    count(distinct j.customer_number) as streamz_without_netflix,
    count(distinct case 
                      when j.cont_mix_curr like '%Netflix%' 
                       and j.cont_mix_curr like '%Streamz%' 
                      then j.customer_number 
                  end) as nbr_nf_on_top_subscribers_m_plus_one
from joined j
group by j.prev_month
order by j.prev_month;


---------------------------------------------------------------------------------------------------------------------------------
--Q18 : What is the number of Play Sports customers per set-top box type? (Sipadan, EOS, Apollo) (join between multiple datasets)
---------------------------------------------------------------------------------------------------------------------------------
SELECT 
    stb.star_snapshot_month_id as snapshot_month,
    CASE 
        WHEN stb.device_product_type IN ('2008C-STB-TN', 'DCX960') THEN 'EOS'
        WHEN stb.device_product_type IN ('CAM1300', 'CI0500-TNO01-31') THEN 'CAM CI+'
        WHEN stb.device_product_type = 'DTVBC_APPEARTV' THEN 'DTV Broadcast Center' --TV Screens in TLN Headquarters used for tests and/or demos
        WHEN stb.device_product_type LIKE 'AD%' THEN 'SIPADAN'
        WHEN stb.device_product_type = 'VIP7002W' THEN 'APOLLO'
    END AS devices_type,
    count(distinct stb.customer_number) AS nbr_customers
FROM presentation_prod.product_offering_explorer.active_inst_base_stb_month stb
left join presentation_prod.value_reporting_engine.active_inst_base_customers_tln ib
on stb.customer_number = ib.customer_number
and cast(replace(ib.month, '-', '') as integer) = stb.star_snapshot_month_id
WHERE 1=1
    AND stb.star_delete_time is null
    AND ib.month = '2025-11' --specify the month here
    AND 
        (
            ib.play_sports_volume > 0 --to filter only Play Sports customers
            --Uncomment below depending on product asked by business
            --or ib.play_more_volume > 0 --to filter only Play More customers
            --or ib.streamz_volume > 0 --to filter only Streamz Premium customers
            --or ib.streamz_plus_volume > 0 --to filter only Streamz Premium+ customers
            --or ib.streamz_basic_volume > 0 --to filter only Streamz Basic customers
            --or ib.netflix_basic_volume > 0 --to filter only Netflix Basic customers
            --or ib.netflix_standard_volume > 0 --to filter only Netflix Standard customers
            --or ib.netflix_premium_volume > 0 --to filter only Netflix Premium customers
            --or ib.disney_plus_volume > 0 --to filter only Disney+ Standard & Premium customers
        )
        
GROUP BY 1, 2
order by 3 desc
;