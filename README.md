# Big Data Project: Raw Data → OLAP

[![Python](https://img.shields.io/badge/python-3.10-blue)](https://www.python.org/) 
[![Spark](https://img.shields.io/badge/spark-3.5.6-orange)](https://spark.apache.org/) 
[![Streamlit](https://img.shields.io/badge/streamlit-latest-green)](https://streamlit.io/) 
[![PowerBI](https://img.shields.io/badge/PowerBI-latest-yellow)](https://powerbi.microsoft.com/) 
[![MySQL](https://img.shields.io/badge/MySQL-latest-lightgrey)](https://www.mysql.com/) 
[![LM Studio](https://img.shields.io/badge/LM%20Studio-latest-purple)](https://lmstudio.io/)

---

## 📖 Table of Contents
1. [Introduction](#introduction)  
2. [High-Level Architecture](#high-level-architecture)  
3. [ETL Batch](#etl-batch)  
   - [log-content](#log-content)  
   - [log-search](#log-search)  
4. [ETL Streaming](#etl-streaming)  
5. [Tech Stack](#tech-stack)  
6. [Achievements](#results)  

---

## 🌟1. Introduction <a name="introduction"></a>
- **Goal**: Transform raw data into OLAP-ready datasets for Customer 360° analysis  
- **Scope**:  
  - **Batch ETL**: process static `log-content` (interaction data) and `log-search` (behaviour data)  
  - **Streaming ETL**: weekly top music video votes in real-time from simulator API  
- **Data Warehouse Design**: applied **RFM** analysis for customer insights  
- **Customer 360°**: provide an overview of customers across all touchpoints

> For full documentation, ETL steps, and in-depth analysis, see [big_data_report.pdf](big_data_report.pdf)

  ![Customer 360](img/customer360.png)  

---

## 🏗 2. High-Level Architecture <a name="high-level-architecture"></a>
- Data flow overview: Raw data → ETL → OLAP / Data Mart → Dashboard  
 
  ![High Level Architecture](img/high-level-architecture.png)  

---

## 💾 3. ETL Batch <a name="etl-batch"></a>
- **ETL process steps**:  
  ![Batch ETL Steps](img/steps_bacth_etl.png)  

### 📌 Stage 1: log-content
- **Design concept**:  
  ![Log Content Star Schema](img/idea_design_log_content_star_schema.png) 
- **Input**: raw interaction logs  
- **Output**: Star Schema Data Warehouse  

### 📌 Stage 2: log-search
- **Design concept**:  
  ![Log Search Flat Table](img/idea_design_log_search_flat_table.png) 
- **Input**: customer search behaviour logs  
- **Output**: Flat Table showing customer search trends  

---

## ⚡ 4. ETL Streaming <a name="etl-streaming"></a>
- **Story**: Each week is a “race” for the most-voted music videos. Spark Streaming continuously ingests votes, calculates real-time counts, and updates the leaderboard on console. Users see which videos are leading and which are gaining votes fast.  
- **Design concept**:  
  ![Stream ETL Design](img/idea_design_stream_ETL.png)  

- **Input**: JSON votes from Simulator API  
- **Output**: Streamlit leaderboard application

---

## 🛠 Tech Stack <a name="tech-stack"></a>

| Component         | Technology / Version       | Purpose                         |
|------------------|----------------------------|---------------------------------|
| Programming       | Python 3.10               | Core scripting                 |
| Batch / Streaming | Spark 3.5.6               | ETL & real-time processing     |
| Dashboard         | Streamlit latest          | Create interactive application |
| BI / Visualization| PowerBI                   | OLAP report dashboards         |
| Database          | MySQL                     | Data Warehouse / Data Mart     |
| ML / Classification| LM Studio                | Log-content classification     |

---

## 🏆 Achievements <a name="results"></a>
- **Batch ETL - log-content**: actual Star Schema output & dashboard  
  ![Log Content Star Schema Output](img/output_log_content_star_schema.png)  
  ![Dashboard Log Content](img/output_log_content_dashboard1.png)  
  ![Dashboard Log Content](img/output_log_content_dashboard2.png)  
- **Batch ETL - log-search**: actual Flat Table output, showing user search trends across different time periods.
  
  ![Log Search Flat Table Output](img/output_log_search_flat_table.png)  
- **Streaming ETL**: real-time leaderboard on console, simulating weekly “music video race”.  
  ![Streamlit UI](img/output_UI_streamlit.png)  
  ![Stream ETL Leaderboard](img/output_stream_ETL.png)  

---

**Author:** Sơn Vũ  
**Date:** 2025