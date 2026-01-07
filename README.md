📌 Databricks Lakehouse Batch Pipeline

This project demonstrates an end-to-end batch data engineering pipeline built using Databricks and Delta Lake, following real production design principles.

🏗️ Architecture
Raw CSV (Bronze)
     ↓
PySpark Transformations (Silver)
     ↓
Delta Lake (Optimized)
     ↓
Gold Aggregations
     ↓
Power BI (Databricks SQL)

🔧 Tech Stack

PySpark

Databricks

Delta Lake

Unity Catalog

SQL

Databricks Jobs (Orchestration)

Power BI

⚙️ Key Features

Parameterized monthly batch processing using dbutils.widgets

Deduplication using Spark window functions

Delta Lake optimization using Z-ORDER

Idempotent Gold layer using MERGE INTO

Automated execution using Databricks Jobs

BI consumption via Power BI

🧠 Engineering Decisions

Batch pipeline designed to be rerunnable and idempotent

Future-ready monthly scheduling even when data is unavailable

Separation of data and checkpoints using Unity Catalog volumes

No blind null filling — data integrity preserved

