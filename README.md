# Building-Scalable-ETL-Pipelines-with-Azure-Databricks-and-Lakeflow-Declarative-Pipelines

## 📌 Overview  
This project implements an **end-to-end data engineering pipeline** for aviation datasets (airports, flights, passengers, bookings) using the **Databricks Lakehouse Platform**.  
It follows the **Medallion Architecture (Bronze → Silver → Gold)** with **Autoloader, Delta Live Tables (DLT), PySpark, SQL**, and supports **incremental loads, schema evolution, CDC, and SCD**.  
Deployed on **Databricks Serverless Compute**, it delivers **analytics-ready models** enabling insights into bookings, flights, passengers, and airport performance.  

---

## 📂 Repository Structure  

```
Building-Scalable-ETL-Pipeline-with-Azure-Databricks-and-Lakeflow-Declarative-Pipelines/
│
├── 📊 Datasets/                # Aviation datasets (CSV/XLS files)
│   ├── dim_airports.csv
│   ├── dim_airports_increment.csv
│   ├── dim_airports_scd.csv
│   ├── dim_flights.csv
│   ├── dim_flights_increment.csv
│   ├── dim_flights_scd.csv
│   ├── dim_passengers.csv
│   ├── dim_passengers_increment.csv
│   ├── dim_passengers_scd.csv
│   ├── fact_bookings.csv
│   └── fact_bookings_increment.csv
│
├── 📜 Docx/                    # Project documentation and diagrams
│   └── Project Architecture.png
│
├── 🐍 Scripts/                 # Python scripts for ingestion, DLT, and transformations
│   ├── bronze_layer.py
│   ├── dlt_pipeline.py
│   ├── gold_dims.py
│   ├── setup.py
│   └── src_parameters.py
│
├── 📑 ProjectReport/           # Final report (Word document)
│   └── report_project2.docx
│
└── 📘 README.md                # Project overview

```

## ⚙️ Tech Stack  
- **Databricks Lakehouse (Serverless Compute)**  
- **Delta Lake**  
- **Autoloader** (incremental ingestion, schema evolution)  
- **Delta Live Tables (DLT)** (transformations, CDC handling, quality checks)  
- **PySpark & SQL** (star schema modeling, SCD, fact/dim tables)

## 🚀 Features  
- Incremental data ingestion with **Autoloader**  
- Schema inference and evolution  
- **CDC and SCD** logic for maintaining up-to-date records  
- Data quality enforcement using DLT expectations  
- Analytics-ready **Star Schema** (fact_bookings with dimension tables)  
- Scalable execution using **Serverless Compute**

