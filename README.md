# 🚀 Facebook Ads Data Pipeline - Azure Synapse & Power BI

## **Automated Data Pipeline for Facebook Ads Using Azure Synapse Analytics & Power BI**

![Power BI Dashboard Preview](https://github.com/YOUR_USERNAME/Facebook-Ads-Data-Pipeline/blob/main/docs/dashboard_preview.png)

This repository contains a complete end-to-end **data pipeline** that automates the extraction, transformation, and reporting of **Facebook Ads performance data**.  
The pipeline is built using **Azure Synapse Analytics**, **Azure Data Lake Storage (ADLS Gen2)**, **Azure Data Factory (ADF)**, and **Power BI** for real-time analytics.

---

## **📌 Project Overview**
This project automates the ingestion and transformation of **Facebook Ads data** into a structured, analytics-ready format using **Azure Synapse Serverless SQL**.  
The data is stored in **ADLS Gen2** and visualized in **Power BI**.

### **🛠️ Technologies Used**
- 🚀 **Azure Synapse Analytics (Serverless SQL)**
- 💾 **Azure Data Lake Storage (ADLS Gen2)**
- 🔄 **Azure Data Factory (for orchestration)**
- 🐍 **PySpark (Azure Synapse Notebooks for ETL)**
- 📊 **Power BI (for reporting and visualization)**
- 📡 **Facebook Ads API (for data extraction)**
- 📁 **Parquet (optimized storage format for Synapse queries)**

---

## **📂 Repository Structure**

📂 Facebook-Ads-Data-Pipeline
│── 📂 notebooks/ # Jupyter Notebooks for ETL process
│ ├── bronze_fact_metrics.ipynb # Extract & load raw data (Bronze Layer)
│ ├── bronze_dim_ad.ipynb
│ ├── bronze_dim_adset.ipynb
│ ├── bronze_dim_campaign.ipynb
│ ├── bronze_dim_platform.ipynb
│ ├── silver_fact_metrics.ipynb # Transform & clean data (Silver Layer)
│ ├── silver_dim_ad.ipynb
│ ├── silver_dim_adset.ipynb
│ ├── silver_dim_campaign.ipynb
│ ├── silver_dim_platform.ipynb
│── 📂 sql_queries/ # SQL scripts for table creation
│ ├── create_external_tables.sql # Creates Synapse external tables
│ ├── create_gold_tables.sql # Creates final Gold Layer tables
│── 📂 powerbi/ # Power BI dataset & reports
│ ├── Facebook_Ads_Dashboard.pbix # Power BI Dashboard file
│── 📂 docs/ # Documentation and setup guides
│ ├── setup_azure_synapse.md # Guide for setting up Synapse
│ ├── setup_powerbi.md # Guide for integrating Power BI
│ ├── data_model.md # Explanation of the data warehouse model
│── 📜 README.md # Project description
│── 📜 LICENSE # License file

---

## **📊 Data Pipeline Architecture**

### **💾 Data Flow**
1️⃣ **Extract Facebook Ads Data**  
Azure Data Factory (ADF) orchestrates API calls to:  
`https://connectors.windsor.ai/facebook?api_key=XYZ`  
Extracts data for different entities (fact_metrics, dim_ad, dim_adset, dim_campaign, dim_platform).

2️⃣ **Load Data into Bronze Layer (Raw Storage - ADLS)**  
Raw JSON responses are stored in ADLS Gen2 (`bronze/` folder).  
Partitioned by date for better query performance.

3️⃣ **Transform Data in Silver Layer (Cleaned & Processed)**  
Data is cast to correct types, duplicates removed, and schema enforced using Synapse Notebooks (PySpark).  
Stored in Parquet format in the `silver/` folder in ADLS.

4️⃣ **Create Gold Layer (Logical Data Warehouse in Synapse SQL)**  
External tables in Synapse Serverless SQL provide structured tables for Power BI.  
Optimized queries using partitioned Parquet storage.

5️⃣ **Power BI Reports & Dashboards**  
Power BI connects directly to Synapse SQL using DirectQuery.  
Dashboards are updated in real-time for tracking campaign performance.

---

## **🛠️ How to Deploy the Project**

### 1️⃣ **Set Up Azure Synapse Analytics**
- Create a Synapse Analytics workspace
- Configure Azure Data Lake Storage (ADLS) and create containers:
  - `bronze/` → Raw JSON files
  - `silver/` → Processed Parquet files
  - `gold/` → Structured tables for Power BI

### 2️⃣ **Run the Notebooks to Process Data**
Use PySpark in Azure Synapse Notebooks:
```python
# Example: Run Bronze Layer Extraction
%run notebooks/bronze_fact_metrics.ipynb
