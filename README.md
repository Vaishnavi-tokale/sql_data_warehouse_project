# Retail Sales SQL Data Warehouse (End-to-End ETL Project)

Welcome to the **Retail Sales SQL Data Warehouse (End-to-End ETL Project)** repository!
This is my first hands-on end-to-end data warehouse project where I explored how retail business data can be transformed into analytics-ready insights using SQL Server.
In this project, I design and implement a complete Retail Sales Data Warehouse using SQL Server, featuring Bronze–Silver–Gold layered architecture, automated ETL pipelines, data quality checks, and analytics-ready modeling for BI insights.

---

## Table of Contents

- Project Overview
- Tech Stack
- Architecture
- Schema Design
- ETL Pipeline
- Key Analytics
- Focus Areas
- Credits
- BI Analytics & Reporting (Data Analytics)
- Final Note

---

## Project Overview

This project simulates a retail business environment where sales data from CRM and ERP systems (CSV files) is integrated into a centralized data warehouse to support analytics and business reporting.

It follows the Medallion Architecture:

- **Bronze Layer**: Raw data ingestion from source CSVs
- **Silver Layer**: Cleaned and standardized data
- **Gold Layer**: Star schema modeled for analytical performance

---

## Tech Stack

- **SQL Server Express** – Data warehouse engine
- **Draw.io** – For architecture and data flow diagrams
- **SSMS** – Querying, ETL development
- - **Star Schema** – Fact & dimension modeling
- **GitHub** – Version control and documentation

---

## How to Run This Project

If you'd like to explore or run this project locally, follow these steps:

### Prerequisites

Make sure you have the following installed:

- SQL Server Express
- SQL Server Management Studio (SSMS)
- Git (optional)

### Steps to Execute

1. Clone the repository

```bash
git clone https://github.com/Mousam25Aarya/sql_data_warehouse_project.git
```

2. Open SQL Server Management Studio (SSMS)

3. Create a new database

4. Run SQL scripts in this order:
   - `scripts/bronze/`
   - `scripts/silver/`
   - `scripts/gold/`

5. Import the CSV files into bronze layer tables

6. Execute analytics queries to generate insights

### Note

This project was built for learning and practicing SQL-based data warehousing concepts including ETL pipelines, layered architecture, and analytical modeling.

## Architecture

The project uses the medallion architecture, a layered approach for separation of concerns and scalability.
![architecture](https://github.com/user-attachments/assets/62b761ff-03d0-4ec2-a352-0d13cde8cdb9)

---

## Schema Design

### Fact Table

- `fact_sales` – Central table for transactions

### Dimension Tables

- `dim_product`
- `dim_customer`
- `dim_store`
- `dim_date`
  The schema follows the **star model**, optimized for fast analytical queries.

---

## ETL Pipeline

The pipeline consists of three stages:

- **Extract**
  Load raw CRM and ERP CSV files into SQL Server bronze tables

- **Transform**
  Clean and prepare data: handling nulls, fixing types, standardizing formats

- **Load**
  Insert cleaned data into silver and finally into gold star schema tables

Scripts are modularized into:

- `scripts/bronze/`
- `scripts/silver/`
- `scripts/gold/`

---

## Key Analytics

Some of the business questions answered by SQL queries:

- Total sales by store and region
- Best-selling products per quarter
- Monthly and seasonal sales trends
- Customer segmentation by purchase frequency

---

## Focus Areas

| Area           | Focus Level    |
| -------------- | -------------- |
| Ingestion      | ██████░░░░ 60% |
| Storage        | █████████░ 90% |
| Modeling       | ████████░░ 80% |
| ETL / ELT      | ██████░░░░ 60% |
| Orchestration  | ███░░░░░░░ 30% |
| Data Quality   | █████░░░░░ 50% |
| Infrastructure | ██████░░░░ 60% |

---

##Credits

- **Inspired by**: @DatawithBaraa’s Data Warehouse Tutorial
- **Developed by**: Vaishnavi Tokale

---

## BI Analytics & Reporting (Data Analytics)

### Objective

Develop SQL-based analytics to deliver detailed insights into:

- **Customer Behavior**
- **Product Performance**
- **Sales Trends**

These insights empower stakeholders with key business metrics, enabling strategic decision-making.

---

## Final Note

"Beyond the technical skills, this project helped me understand how to structure a data engineering solution entirely with SQL from raw data to business-ready analytics.
Working on this project also helped me understand the importance of proper documentation, structured planning, and clean data architecture in real-world projects.

---

### License

This project is licensed under the [MIT License](LICENSE). You are free to use, modify and share this project with proper attribution.
