# 🚀 Facebook Ads Data Pipeline - Azure Synapse & Power BI

## **Automated Data Pipeline for Facebook Ads Using Azure Synapse Analytics & Power BI**

![Power BI Dashboard Preview](https://github.com/YOUR_USERNAME/Facebook-Ads-Data-Pipeline/blob/main/docs/dashboard_preview.png)

This repository contains a complete end-to-end **data pipeline** that automates the extraction, transformation, and reporting of **Facebook Ads performance data**.  
The pipeline is built using **Azure Synapse Analytics**, **Azure Data Lake Storage (ADLS Gen2)**, **Azure Data Factory (ADF)**, and **Power BI** for real-time analytics.

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

## **📂 Repository Structure**
```plaintext
📂 Facebook-Ads-Data-Pipeline
│── 📂 notebooks/                     # Jupyter Notebooks for ETL process
│   ├── bronze_fact_metrics.ipynb       # Extract & load raw data (Bronze Layer)
│   ├── bronze_dim_ad.ipynb             
│   ├── bronze_dim_adset.ipynb          
│   ├── bronze_dim_campaign.ipynb       
│   ├── bronze_dim_platform.ipynb       
│   ├── silver_fact_metrics.ipynb       # Transform & clean data (Silver Layer)
│   ├── silver_dim_ad.ipynb             
│   ├── silver_dim_adset.ipynb          
│   ├── silver_dim_campaign.ipynb       
│   ├── silver_dim_platform.ipynb       
│── 📂 sql_queries/                   # SQL scripts for table creation
│   ├── create_external_tables.sql      # Creates Synapse external tables
│   ├── create_gold_tables.sql          # Creates final Gold Layer tables
│── 📂 powerbi/                        # Power BI dataset & reports
│   ├── Facebook_Ads_Dashboard.pbix     # Power BI Dashboard file
│── 📂 docs/                           # Documentation and setup guides
│   ├── setup_azure_synapse.md          # Guide for setting up Synapse
│   ├── setup_powerbi.md                # Guide for integrating Power BI
│   ├── data_model.md                   # Explanation of the data warehouse model
│── 📜 README.md                        # Project description
│── 📜 LICENSE                           # License file
```
