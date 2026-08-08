# End-to-End Retail Data Pipeline & Analytics in Microsoft Fabric

## Workspace Architecture & Asset Summary

Workspace: ws-nz-retail-dev

ws-nz-retail-dev/
├── retail_lakehouse (Lakehouse)
│ ├── Files/
│ │ └── bronze/
│ │ ├── inventory_raw.txt
│ │ ├── orders_raw.json
│ │ └── returns_raw.csv
│ └── Tables/
│ └── dbo/
│ ├── orders_silver
│ ├── returns_silver
│ ├── inventory_silver
│ └── gold_order_analytics
├── retail_pipeline (Child Ingestion Pipeline)
├── nb_medallion_processing (PySpark Notebook)
├── Order_Analytics_Model (Semantic Model)
├── Executive_Order_Analytics_Dashboard (Power BI Report)
└── pl_master_daily_orchestrator (Parent Orchestration Pipeline)

# Technical Pipeline Workflow

## Step 1: HTTP Ingestion via Child Pipeline (retail_pipeline)

Source: GitHub raw files (HTTP endpoint).

Execution: A ForEach activity loops over source file metadata.

Target: Downloads multi-format raw files directly into OneLake storage under Files/bronze/:

orders_raw.json (JSON format)
returns_raw.csv (CSV format)
inventory_raw.txt (Pipe-delimited | text format)

## Step 2: Medallion Processing in PySpark (nb_medallion_processing)

1. Bronze Ingestion

PySpark reads raw files using implicit schema inference:

df_orders_bronze = spark.read.json("Files/bronze/orders_raw.json")
df_returns_bronze = spark.read.option("header", "true").csv("Files/bronze/returns_raw.csv")
df_inventory_bronze = spark.read.option("header", "true").option("delimiter", "|").csv("Files/bronze/inventory_raw.txt")

2. Silver Transformations

Data cleaning and explicit type casting applied before writing Delta Lake tables (overwrite mode):

orders_silver: Casts Order_ID and cust_id to long, Qty to integer, extracts numeric value from Order_Amount$ to double, standardizes whitespace and string casing on Product_Name and Delivery_Status (e.g., mapping "delivrd" $\rightarrow$ "delivered"), and formats Order_Date to yyyy-MM-dd.

returns_silver: Casts Return_ID and Order_ID to long, Return_Amount to double, standardizes Product and Return_Reason strings, and formats Return_Date to yyyy-MM-dd.

inventory_silver: Standardizes whitespace and lowercases Product_Name, casting Stock_Available to integer and Unit_Cost_USD to double.

3. Gold Aggregations & Data Quality Assertions

Enriched Join: Left joins orders_silver with returns_silver on Order_ID and inventory_silver on Product_Name.

Calculated Metrics:

Net_Revenue = coalesce(Order_Amount, 0.0) - coalesce(Return_Amount, 0.0)
Total_Cost = Qty \* coalesce(Unit_Cost_USD, 0.0)
Profit = Net_Revenue - Total_Cost

Embedded Assertions: Validates row counts (assert actual_rows == 10), verifies primary key integrity (Order_ID IS NOT NULL), audits missing values, and checks math consistency across all records before overwriting gold_order_analytics.

KPI Calculations: Computes executive summaries (Gross_Net_Revenue, Gross_Total_Cost, Gross_Profit, Profit_Margin_Pct) and product profitability rankings (Product_Profit).

_Lakehouse & Storage Structure Screenshot_

### Lakehouse & Storage Architecture

![Lakehouse File & Delta Table Structure](screenshots/lakehouse_files_structure.png)

## Step 3: Semantic Modeling & Reporting

Semantic Model: Order_Analytics_Model built directly on top of gold_order_analytics.

Reporting: Executive_Order_Analytics_Dashboard visualizes revenue performance, product profit rankings, unit volumes, and order status breakdowns.

## Step 4: Master Orchestration (pl_master_daily_orchestrator)

Automates the batch pipeline sequence:

Master Orchestration Pipeline Screenshot

### Master Daily Orchestration Pipeline

![Master Daily Orchestration Pipeline](screenshots/orchestration_pipeline.png)

Invoke Pipeline (Ingest_Retail_Data): Executes retail_pipeline to fetch raw files from GitHub via HTTP into Files/bronze/.

Notebook Execution (Run_Gold_ETL_and_Assertions): On ingestion success, triggers nb_medallion_processing to run cleaning, Silver writes, Gold calculations, and quality assertions.

Semantic Model Refresh (Refresh_Order_Analytics Model): On notebook success, refreshes Order_Analytics_Model to update downstream dashboard visuals.
