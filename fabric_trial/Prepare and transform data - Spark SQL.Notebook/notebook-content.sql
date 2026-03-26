-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "69071d2d-c649-43fa-b728-6a0b78864a38",
-- META       "default_lakehouse_name": "wwilakehouse",
-- META       "default_lakehouse_workspace_id": "4c10fb2e-638f-4197-a8f2-b0ae64c73592",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "69071d2d-c649-43fa-b728-6a0b78864a38"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- MARKDOWN ********************

-- # Prepare and transform data in the lakehouse (Spark SQL)
-- 
-- This is a companion notebook for the [Microsoft Learn tutorial](https://learn.microsoft.com/fabric/data-engineering/tutorial-lakehouse-data-preparation) that follows Path 1 in this notebook.
-- 
-- This notebook includes two execution paths:
-- - Path 1: Lakehouse schemas enabled (`Tables/dbo/...`) — this is the supported path for the tutorial article.
-- - Path 2: Lakehouse schemas not enabled (`wwilakehouse....`) — use this alternate path when schemas are not enabled in your environment.

-- MARKDOWN ********************

-- ## Path 1 - Lakehouse schemas enabled (tutorial-supported path)
-- ### Create Delta tables
-- Run these cells to create Delta tables from raw data using schema-qualified paths (`Tables/dbo/...`).

-- MARKDOWN ********************

-- #### Cell 1 - Spark session configuration
-- This cell enables two Fabric features that optimize how data is written and read in subsequent cells. V-order optimizes parquet layout for faster reads and better compression. Optimize Write reduces the number of files written and increases individual file size.

-- CELL ********************

-- MAGIC %%sql
-- MAGIC SET spark.sql.parquet.vorder.enabled=true;
-- MAGIC SET spark.microsoft.delta.optimizeWrite.enabled=true;
-- MAGIC SET spark.microsoft.delta.optimizeWrite.binSize=1073741824;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- #### Cell 2 - Fact - Sale
-- This cell reads raw parquet data from `Files/wwi-raw-data/full/fact_sale_1y_full`, adds date part columns (`Year`, `Quarter`, and `Month`), and writes `fact_sale` as a Delta table partitioned by `Year` and `Quarter`.

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE OR REPLACE TABLE delta.`Tables/dbo/fact_sale`
-- MAGIC USING DELTA
-- MAGIC PARTITIONED BY (Year, Quarter)
-- MAGIC AS
-- MAGIC SELECT
-- MAGIC    *,
-- MAGIC    year(InvoiceDateKey) AS Year,
-- MAGIC    quarter(InvoiceDateKey) AS Quarter,
-- MAGIC    month(InvoiceDateKey) AS Month
-- MAGIC FROM parquet.`Files/wwi-raw-data/full/fact_sale_1y_full`;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- #### Cell 3 - Dimensions
-- This cell reads the five dimension parquet datasets and writes them as Delta tables (`dimension_city`, `dimension_customer`, `dimension_date`, `dimension_employee`, and `dimension_stock_item`) under `Tables/dbo/...`.

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE OR REPLACE TABLE delta.`Tables/dbo/dimension_city` USING DELTA AS SELECT * FROM parquet.`Files/wwi-raw-data/full/dimension_city`;
-- MAGIC CREATE OR REPLACE TABLE delta.`Tables/dbo/dimension_customer` USING DELTA AS SELECT * FROM parquet.`Files/wwi-raw-data/full/dimension_customer`;
-- MAGIC CREATE OR REPLACE TABLE delta.`Tables/dbo/dimension_date` USING DELTA AS SELECT * FROM parquet.`Files/wwi-raw-data/full/dimension_date`;
-- MAGIC CREATE OR REPLACE TABLE delta.`Tables/dbo/dimension_employee` USING DELTA AS SELECT * FROM parquet.`Files/wwi-raw-data/full/dimension_employee`;
-- MAGIC CREATE OR REPLACE TABLE delta.`Tables/dbo/dimension_stock_item` USING DELTA AS SELECT * FROM parquet.`Files/wwi-raw-data/full/dimension_stock_item`;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ### Transform data for business aggregates
-- Run the transformation cells to create aggregate outputs for reporting in the schema-enabled path.

-- MARKDOWN ********************

-- #### Cell 4 - Load source tables
-- For Spark SQL, no additional load step is required in this notebook. The source Delta tables are used directly in the SQL statements below.

-- MARKDOWN ********************

-- #### Cell 5 - Aggregate sale by date and city
-- This cell computes monthly sales totals by city and materializes the result as `aggregate_sale_by_date_city` using `CREATE OR REPLACE TABLE ... AS SELECT`.

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE OR REPLACE TABLE delta.`Tables/dbo/aggregate_sale_by_date_city`
-- MAGIC AS
-- MAGIC SELECT
-- MAGIC     fs.Year,
-- MAGIC     fs.Month,
-- MAGIC     c.City,
-- MAGIC     c.StateProvince,
-- MAGIC     c.SalesTerritory,
-- MAGIC     SUM(fs.TotalExcludingTax) AS sum_of_total_excluding_tax,
-- MAGIC     SUM(fs.TaxAmount) AS sum_of_tax_amount,
-- MAGIC     SUM(fs.Profit) AS sum_of_profit
-- MAGIC FROM delta.`Tables/dbo/fact_sale` fs
-- MAGIC INNER JOIN delta.`Tables/dbo/dimension_city` c
-- MAGIC     ON fs.CityKey = c.CityKey
-- MAGIC GROUP BY
-- MAGIC     fs.Year,
-- MAGIC     fs.Month,
-- MAGIC     c.City,
-- MAGIC     c.StateProvince,
-- MAGIC     c.SalesTerritory

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- #### Cell 6 - Aggregate sale by date and employee
-- This cell computes monthly sales totals by employee and writes the result to `aggregate_sale_by_date_employee` using `CREATE OR REPLACE TABLE ... AS SELECT`.

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE OR REPLACE TABLE delta.`Tables/dbo/aggregate_sale_by_date_employee`
-- MAGIC AS
-- MAGIC SELECT
-- MAGIC     fs.Year,
-- MAGIC     fs.Month,
-- MAGIC     e.Employee,
-- MAGIC     e.IsSalesperson,
-- MAGIC     SUM(fs.TotalExcludingTax) AS sum_of_total_excluding_tax,
-- MAGIC     SUM(fs.TaxAmount) AS sum_of_tax_amount,
-- MAGIC     SUM(fs.Profit) AS sum_of_profit
-- MAGIC FROM delta.`Tables/dbo/fact_sale` fs
-- MAGIC INNER JOIN delta.`Tables/dbo/dimension_employee` e
-- MAGIC     ON fs.SalespersonKey = e.EmployeeKey
-- MAGIC GROUP BY
-- MAGIC     fs.Year,
-- MAGIC     fs.Month,
-- MAGIC     e.Employee,
-- MAGIC     e.IsSalesperson

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ## Path 2 - Lakehouse schemas not enabled (alternate path)
-- ### Create Delta tables
-- Run these cells to create Delta tables from raw data using non-schema table names (`wwilakehouse....`).
-- 
-- #### Cell 1 - Spark session configuration
-- This cell enables two Fabric features that optimize how data is written and read in subsequent cells. V-order optimizes parquet layout for faster reads and better compression. Optimize Write reduces the number of files written and increases individual file size.

-- CELL ********************

-- MAGIC %%sql
-- MAGIC SET spark.sql.parquet.vorder.enabled=true;
-- MAGIC SET spark.microsoft.delta.optimizeWrite.enabled=true;
-- MAGIC SET spark.microsoft.delta.optimizeWrite.binSize=1073741824;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- #### Cell 2 - Fact - Sale
-- This cell reads raw parquet data from `Files/wwi-raw-data/full/fact_sale_1y_full`, adds date part columns (`Year`, `Quarter`, and `Month`), and writes `wwilakehouse.fact_sale` as a Delta table partitioned by `Year` and `Quarter`.

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE OR REPLACE TABLE wwilakehouse.fact_sale
-- MAGIC USING DELTA
-- MAGIC PARTITIONED BY (Year, Quarter)
-- MAGIC AS
-- MAGIC SELECT
-- MAGIC    *,
-- MAGIC    year(InvoiceDateKey) AS Year,
-- MAGIC    quarter(InvoiceDateKey) AS Quarter,
-- MAGIC    month(InvoiceDateKey) AS Month
-- MAGIC FROM parquet.`Files/wwi-raw-data/full/fact_sale_1y_full`;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- #### Cell 3 - Dimensions
-- This cell reads the five dimension parquet datasets and writes them as Delta tables (`dimension_city`, `dimension_customer`, `dimension_date`, `dimension_employee`, and `dimension_stock_item`) under `wwilakehouse....`.

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE OR REPLACE TABLE wwilakehouse.dimension_city USING DELTA AS SELECT * FROM parquet.`Files/wwi-raw-data/full/dimension_city`;
-- MAGIC CREATE OR REPLACE TABLE wwilakehouse.dimension_customer USING DELTA AS SELECT * FROM parquet.`Files/wwi-raw-data/full/dimension_customer`;
-- MAGIC CREATE OR REPLACE TABLE wwilakehouse.dimension_date USING DELTA AS SELECT * FROM parquet.`Files/wwi-raw-data/full/dimension_date`;
-- MAGIC CREATE OR REPLACE TABLE wwilakehouse.dimension_employee USING DELTA AS SELECT * FROM parquet.`Files/wwi-raw-data/full/dimension_employee`;
-- MAGIC CREATE OR REPLACE TABLE wwilakehouse.dimension_stock_item USING DELTA AS SELECT * FROM parquet.`Files/wwi-raw-data/full/dimension_stock_item`;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ### Transform data for business aggregates
-- Run the transformation cells to create aggregate outputs for reporting in the non-schema path.

-- MARKDOWN ********************

-- #### Cell 4 - Load source tables
-- This step prepares the source tables for aggregation in Path 2. For Spark SQL, no additional load statement is required because the SQL queries reference Delta tables directly.

-- MARKDOWN ********************

-- #### Cell 5 - Aggregate sale by date and city
-- This cell computes monthly sales totals by city and writes the result to `wwilakehouse.aggregate_sale_by_date_city`.

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE OR REPLACE TABLE wwilakehouse.aggregate_sale_by_date_city
-- MAGIC AS
-- MAGIC SELECT
-- MAGIC     fs.Year,
-- MAGIC     fs.Month,
-- MAGIC     c.City,
-- MAGIC     c.StateProvince,
-- MAGIC     c.SalesTerritory,
-- MAGIC     SUM(fs.TotalExcludingTax) AS sum_of_total_excluding_tax,
-- MAGIC     SUM(fs.TaxAmount) AS sum_of_tax_amount,
-- MAGIC     SUM(fs.Profit) AS sum_of_profit
-- MAGIC FROM wwilakehouse.fact_sale fs
-- MAGIC INNER JOIN wwilakehouse.dimension_city c
-- MAGIC     ON fs.CityKey = c.CityKey
-- MAGIC GROUP BY
-- MAGIC     fs.Year,
-- MAGIC     fs.Month,
-- MAGIC     c.City,
-- MAGIC     c.StateProvince,
-- MAGIC     c.SalesTerritory

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- #### Cell 6 - Aggregate sale by date and employee
-- This cell computes monthly sales totals by employee and writes the result to `wwilakehouse.aggregate_sale_by_date_employee`.

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE OR REPLACE TABLE wwilakehouse.aggregate_sale_by_date_employee
-- MAGIC AS
-- MAGIC SELECT
-- MAGIC     fs.Year,
-- MAGIC     fs.Month,
-- MAGIC     e.Employee,
-- MAGIC     e.IsSalesperson,
-- MAGIC     SUM(fs.TotalExcludingTax) AS sum_of_total_excluding_tax,
-- MAGIC     SUM(fs.TaxAmount) AS sum_of_tax_amount,
-- MAGIC     SUM(fs.Profit) AS sum_of_profit
-- MAGIC FROM wwilakehouse.fact_sale fs
-- MAGIC INNER JOIN wwilakehouse.dimension_employee e
-- MAGIC     ON fs.SalespersonKey = e.EmployeeKey
-- MAGIC GROUP BY
-- MAGIC     fs.Year,
-- MAGIC     fs.Month,
-- MAGIC     e.Employee,
-- MAGIC     e.IsSalesperson

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
