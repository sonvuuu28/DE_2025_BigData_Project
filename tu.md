# A. **Introduction**

## Project Overview
This project demonstrates ETL to transform raw data into OLAP-ready outputs. ETL is performed in batch and streaming modes.

**What are the inputs and outputs of this project?**

## 1. Batch ETL
**Input**: log-search and log-content folders (customer searches and contract interactions).
**Output**: OLAP output includes Data Warehouse and Data Mart for reporting and Customer 360° insights.

### Customer 360
A Customer 360° view provides a unified and comprehensive understanding of each customer across all interactions and touchpoints.It enables businesses to personalize services, improve customer satisfaction, strengthen retention, and identify cross-sell or up-sell opportunities—ultimately driving better decision-making and increasing revenue. 

![ảnh minh họa customer 360](img/image.png)

## 2. Streaming ETL
**Input**: Simulator API provided by the business, continuously sending song vote data in JSON format. Spark Streaming ingests his data in real time, processing votes as they arrive.
**Output**: Small Streamlit app that displays a live votes leaderboard, showing which songs are leading based on real-time aggregated counts computed by Spark Streaming.

# **B. High Level Architecture**
![alt text](img/high-level-architecture.png)

# **C. ETL Batch Data**
## Steps
![alt text](img/C%20steps.png)