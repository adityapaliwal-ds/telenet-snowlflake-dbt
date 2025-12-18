# Original Analysis Queries

This folder contains dbt analysis files converted from the original GenBIPOC Commercial Data Queries.

## What are dbt Analysis Files?

Analysis files in dbt:
- Get **compiled** (Jinja templating, `{{ source() }}`, `{{ ref() }}` are resolved)
- Do **NOT get executed** by `dbt run`
- Are perfect for ad-hoc queries, exploration, or one-off analysis

## How to Use These Files

### 1. Compile a query
```bash
dbt compile --select q01a_install_base_evolution_customer_level
```

### 2. Find the compiled SQL
The compiled query will be in:
```
target/compiled/telenet_shift_left/analyses/original/q01a_install_base_evolution_customer_level.sql
```

### 3. Run in Snowflake
- Open the compiled file
- Copy the SQL (all `{{ source() }}` references will be replaced with actual table names)
- Paste into Snowflake and run

## Best Practices Applied

All queries have been converted to use dbt best practices:

### Source References
Instead of hardcoded table names:
```sql
-- OLD (hardcoded)
from presentation_prod.value_reporting_engine.active_inst_base_customers_tln

-- NEW (using source)
from {{ source('value_reporting_engine', 'active_inst_base_customers_tln') }}
```

This approach provides:
- **Centralized source definitions** in [models/sources.yml](../../models/sources.yml)
- **Better maintainability** - change schema/database in one place
- **Data lineage tracking** in dbt docs
- **Source freshness checks** (can be added later)

## Query Index

### Install Base & Evolution (Q1)
- [q01a_install_base_evolution_customer_level.sql](q01a_install_base_evolution_customer_level.sql) - Customer level install base
- [q01b_install_base_evolution_household_level.sql](q01b_install_base_evolution_household_level.sql) - Household level install base

### Attach Rates (Q2)
- [q02_entertainment_attach_rate.sql](q02_entertainment_attach_rate.sql) - Entertainment attach rate for TLN and DTV

### Sales & Churn (Q3-Q4)
- [q03a_churn_by_product.sql](q03a_churn_by_product.sql) - Churn by entertainment product
- [q03b_sales_by_product.sql](q03b_sales_by_product.sql) - Sales by entertainment product
- [q04_churn_by_sales_channel.sql](q04_churn_by_sales_channel.sql) - Churn by sales channel

### Product Mix Analysis (Q5)
- [q05a_avg_products_per_household.sql](q05a_avg_products_per_household.sql) - Average products per HH
- [q05b_households_by_product_count.sql](q05b_households_by_product_count.sql) - HH distribution by product count
- [q05c_avg_products_per_customer.sql](q05c_avg_products_per_customer.sql) - Average products per customer
- [q05d_customers_by_product_count.sql](q05d_customers_by_product_count.sql) - Customer distribution by product count

### Day Pass & Channel Analysis (Q6-Q7)
- [q06_daypass_sales.sql](q06_daypass_sales.sql) - Play Sports day pass sales
- [q07_digital_channel_sales_share.sql](q07_digital_channel_sales_share.sql) - TV Shop vs Digital Web/App sales share

### Customer Profiling (Q8)
- [q08a_non_dtv_customers_lifestage.sql](q08a_non_dtv_customers_lifestage.sql) - Non-DTV customer lifestage
- [q08b_non_dtv_customers_product_subscriptions.sql](q08b_non_dtv_customers_product_subscriptions.sql) - Non-DTV product subscriptions
- [q08c_non_dtv_customers_behavioral_segmentation.sql](q08c_non_dtv_customers_behavioral_segmentation.sql) - Non-DTV behavioral segments

### Advanced Analytics (Q9-Q12)
- [q09_play_sports_churners_dtv_churn.sql](q09_play_sports_churners_dtv_churn.sql) - Play Sports churners who also churned DTV
- [q10_customers_with_5pct_discount.sql](q10_customers_with_5pct_discount.sql) - Customers with 5% marketplace discount
- [q11a_cancellations_lines_level.sql](q11a_cancellations_lines_level.sql) - Line-level cancellations
- [q11b_cancellations_entertainment_products.sql](q11b_cancellations_entertainment_products.sql) - Entertainment product cancellations
- [q12_cam_ci_plus_customers_devices.sql](q12_cam_ci_plus_customers_devices.sql) - CAM CI+ customers and devices

### Promo Analysis (Q13-Q14)
- [q13_promo_subscribers_note.sql](q13_promo_subscribers_note.sql) - NOTE ONLY: Redirect to analysts
- [q14_promo_retention_note.sql](q14_promo_retention_note.sql) - NOTE ONLY: Redirect to analysts

### Customer Journey Analysis (Q15-Q18)
- [q15_new_internet_play_sports_uptake.sql](q15_new_internet_play_sports_uptake.sql) - New internet customers taking Play Sports
- [q16_new_internet_dtv_streamz_then_netflix.sql](q16_new_internet_dtv_streamz_then_netflix.sql) - New Int/DTV → Streamz → Netflix journey
- [q17_streamz_customers_adding_netflix.sql](q17_streamz_customers_adding_netflix.sql) - Streamz customers adding Netflix on top
- [q18_play_sports_customers_by_stb_type.sql](q18_play_sports_customers_by_stb_type.sql) - Play Sports customers by STB device type

## Notes

- All queries exclude technical migrations from 2025 where applicable
- Date filters can be adjusted in each query (look for comments with "specify the month/period here")
- Some queries are marked as "RED" complexity and should be handled by analysts (Q13, Q14)
