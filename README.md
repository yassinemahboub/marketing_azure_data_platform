# 🚀 Marketing Data Platform - Azure Synapse & Power BI



## **📌 Project Overview**
This project automates the ingestion and transformation of **Facebook Ads data** into a structured, analytics-ready format using **Azure Synapse Serverless SQL**. 
 
The data is stored in **ADLS Gen2** and visualized in **Power BI**.

### **🛠️ Technologies Used**
- 🚀 **Azure Synapse Analytics (Serverless SQL)**
- 💾 **Azure Data Lake Storage (ADLS Gen2)**
- 🔄 **Azure Data Factory (for orchestration)**
- 🐍 **PySpark (Azure Synapse Notebooks for ETL)**
- 📊 **Power BI (for reporting and visualization)**
- 📡 **Windsor.ai Connector API for Facebook Ads (for data extraction)**
- 📁 **Parquet (optimized storage format for Synapse queries)**

## **📂 Repository Structure**
```plaintext
│── 📂 notebooks/                     # Spark Notebooks for Data Extraction
│   ├── 📂 bronze/          
│       ├── bronze_fact_metrics.ipynb       
│       ├── bronze_dim_ad.ipynb             
│       ├── bronze_dim_adset.ipynb          
│       ├── bronze_dim_campaign.ipynb       
│       ├── bronze_dim_platform.ipynb
│   ├── 📂 silver/        
│       ├── silver_fact_metrics.ipynb       
│       ├── silver_dim_ad.ipynb             
│       ├── silver_dim_adset.ipynb          
│       ├── silver_dim_campaign.ipynb       
│       ├── silver_dim_platform.ipynb       
│── 📂 sql_queries/                   # SQL scripts for table creation
│   ├── create_external_tables.sql      
│   ├── initial_setup.sql          
│── 📂 powerbi/                        # Power BI dataset & reports
│   ├── Facebook_Ads_Dashboard.pbix     
│── 📂 docs/                           # Documentation and setup guides
│   ├── data_model.md                   
│── 📜 README.md                        # Project description

```

# 📐 Architectural Components & Design Principles

## 1️⃣ Data Ingestion Layer (Batch & Incremental Ingestion)

**Orchestrated by Azure Data Factory (ADF):**
- Uses REST API connectors to extract data from the Facebook Ads API.
- Implements parameterized pipelines for historical backfill and incremental daily refresh.
- Ensures fault tolerance with retry logic and logging mechanisms.

**Raw Data Storage:**
- Data is ingested into Azure Data Lake Storage Gen2 (ADLS) in the Bronze Layer.
- Stored in JSON format to retain raw API responses for future reprocessing if needed.
- Data is partitioned by date to optimize query performance.

## 2️⃣ Data Processing Layer (ETL & Schema Enforcement)

**Processing Engine: Azure Synapse Analytics (Apache Spark Pools)**
- Uses PySpark notebooks to cleanse, normalize, and transform data into a structured format.
- Applies schema enforcement and data type casting to ensure consistency.
- Implements idempotency to prevent data duplication during incremental loads.

**Bronze → Silver Transformation (Schema Normalization & Data Cleaning):**
- Converts JSON to Parquet format for high-performance query execution.
- Performs type casting (e.g., string → date, float → int).
- Handles slowly changing dimensions (SCDs) for dimensions like ad_campaigns and adsets.
- Enforces business rules by removing invalid records to ensure data integrity.

## 3️⃣ Data Warehousing Layer (Analytical Storage - Gold Layer)

**Logical Data Warehouse in Azure Synapse Serverless SQL:**
- Structured as a star schema with fact tables and dimension tables for optimized reporting.
- Uses external tables to query Parquet files stored in ADLS without duplicating data.
- Implements a partitioning strategy to improve query performance using date-based partitions.

**Fact & Dimension Table Modeling:**
- **fact_facebook_ads_metrics:** Stores aggregated ad performance data (clicks, impressions, spend, etc.).
- **dim_ad:** Stores metadata about each advertisement.
- **dim_adset:** Contains ad set configurations (targeting, budget, etc.).
- **dim_campaign:** Tracks campaign-level performance and objectives.
- **dim_platform:** Stores information on ad placement across different publisher platforms.

## 4️⃣ Data Consumption & Analytics (Power BI Reporting)

**DirectQuery Mode for Real-Time Analysis:**
- Power BI connects to Synapse Serverless SQL via DirectQuery for real-time analytics.
- Allows business users to filter, aggregate, and analyze ad performance data interactively.

**Optimized Data Model for BI:**
- Joins between fact and dimension tables ensure faster query performance.
- Pre-aggregated metrics and KPIs (CTR, CPM, CPA) improve visualization speed.

