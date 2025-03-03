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

## 1️⃣ Data Ingestion: Windsor.ai API Integration

- **Data Source:** Facebook Ads performance data is retrieved via Windsor.ai's API.
- **API Configuration:** The API endpoint dynamically pulls key performance metrics.
- **Orchestration:** Azure Data Factory (ADF) schedules and automates API calls.
- **Authentication:** Uses API keys for secure data retrieval.

### 📌 Example API Request (Used in Synapse Notebooks)

```python
url = https://connectors.windsor.ai/facebook?api_key=YOUR_API_KEY
&date_preset=last_7d&fields=ad_id,ad_name,adcontent,ad_created_time
```

- Extracts campaign-level and ad-level performance data.
- Supports date filtering to fetch historical and incremental data.
- Configured as a parameterized request to allow dynamic extraction in Synapse Pipelines.

## 2️⃣ Data Storage: Bronze Layer (Raw Storage)

- Extracted JSON data from Windsor.ai API is stored in Azure Data Lake Storage (ADLS Gen2).

- **Storage Format:** JSON files (for raw ingestion) → Parquet files (for efficient querying).

- **Partition Strategy:**
  - Partitioned by Year/Month/Day for optimized query performance.

  - Enables incremental data ingestion.

### 📌 Storage Example

```plaintext
/bronze/facebook_ads/2024/03/01/facebook_ads_data.json
```
## 3️⃣ Data Processing: Silver Layer (ETL & Schema Enforcement)

- **Processing Engine:** Azure Synapse Apache Spark (PySpark) Notebooks.
- **Transformations:**
  - Schema enforcement: Convert raw JSON fields into structured tables.
  - Data type casting: Ensure float, int, datetime conversions.
  - Data deduplication & validation.

### Example PySpark Transformation in Synapse

```python
from pyspark.sql.functions import col, to_date

df_silver = df_bronze.select(
    to_date(col("date"), "yyyy-MM-dd").alias("date"),
    col("campaign_id").cast("string"),
    col("ad_id").cast("string"),
    col("clicks").cast("int"),
    col("spend").cast("float"),
    col("impressions").cast("int")
)
```
- **Schema Normalization:** Converts API fields into structured tables.

- **Parquet Format Optimization:** Reduces storage costs and improves query speed.

### Processed Data Example in ADLS (Silver Layer)

```plaintext
/silver/facebook_ads/fact_facebook_ads_metrics/2024/03/01/data.parquet
```
## 4️⃣ Data Warehouse: Gold Layer (Synapse SQL - External Tables)

- **Fact & Dimension Table Design:**
  - `fact_facebook_ads_metrics` (Aggregated Ads Performance)
  - `dim_campaign` (Campaign Details)
  - `dim_adset` (Ad Set Metadata)
  - `dim_ad` (Ad-Level Attributes)
  - `dim_platform` (Placement & Device Info)

### 📌 Example: Create External Table in Synapse SQL

```sql
USE facebook_ads

-- CREATE EXTERNAL FACT TABLE

CREATE EXTERNAL TABLE dbo.fact_metrics (
	[date] date,
	[ad_id] nvarchar(4000),
	[adset_id] nvarchar(4000),
	[campaign_id] nvarchar(4000),
	[account_id] nvarchar(4000),
	[placement] nvarchar(4000),
	[platform] nvarchar(4000),
	[spend] real,
	[impressions] int,
	[clicks] int,
	[frequency] real,
	[leads] int,
	[ad_engagement] int,
	[video_views] int,
	[thruplay_video_views] int,
	[load_date] datetime2(7),
	[source] nvarchar(4000)
	)
	WITH (
	LOCATION = 'facebook_ads/fact_facebook_ads_metrics/**',
	DATA_SOURCE = silver,
	FILE_FORMAT = ParquetFormat
	)
GO
```
- **Directly queries Parquet files from ADLS** without duplicating data.
- **Supports partition pruning** for optimized performance.

## 5️⃣ Business Intelligence: Power BI Dashboard

- **DirectQuery Mode:** Enables real-time analysis by querying Synapse SQL tables directly.
- **Pre-aggregated Metrics & KPIs:**
  - **Cost Per Click (CPC):** `spend / clicks`
  - **Click-Through Rate (CTR):** `(clicks / impressions) * 100`
  - **Conversion Rate:** `(actions_lead / clicks) * 100`
- Filters & Slicers allow users to drill down into campaign performance.

## Example Power BI Dataset Model
*(Provide your model details here)*
