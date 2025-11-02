# Eurowings Scala Challenge – Task 1  
### Spark Data Ingestion and Preparation

## 1️⃣ Overview
This Spark / Scala application ingests the CSV files provided by the external partner every two months, cleans the data, and prepares it for analytical use.  

Each delivery file follows the naming format:

DatasetName_YYYYMMDD.csv


For example:
Vertriebskanal_20250314.csv
Airports_20250314.csv
Reiseveranstalter_20250314.csv


The goal is to build a simple but reliable data-ingestion pipeline that stores data safely in a **Data Lake**, applies basic cleaning, and makes it ready for analytics and reporting.

---

## 2️⃣ Technology Stack
- **Language:** Scala 2.12  
- **Framework:** Apache Spark 3.5.1  
- **Build tool:** sbt  
- **Configuration:** Typesafe Config  
- **Storage format:** Parquet or Delta (optional)  
- **Environment:** Local Spark or Databricks cluster  

---

## 3️⃣ File Naming and Data-Lake Structure
**Input files:** arrive as  

DatasetName_YYYYMMDD.csv

**Examples:**  
- Vertriebskanal_20250314.csv  
- Airports_20250314.csv  
- Reiseveranstalter_20250314.csv  

**Storage pattern in the Data Lake:**  

/bronze/<dataset_name>/ingestion_date=<yyyy-mm-dd>/


**Examples:**  
- /bronze/Vertriebskanal/ingestion_date=2025-03-14  
- /bronze/Airports/ingestion_date=2025-03-14  

This folder structure keeps the data organized by dataset and load date.

---

## 4️⃣ Lakehouse Medallion Architecture
The pipeline follows a simplified **Bronze → Silver → Gold** structure:

| Layer | Purpose | Example in this project |
|-------|----------|-------------------------|
| **Bronze** | Stores raw CSVs exactly as received. | `/bronze` folders with ingestion date partition. |
| **Silver** | Cleans and standardizes the schema and data types. | Renames columns, casts numeric types, handles null values. |
| **Gold** | Business-ready analytics tables and KPIs. | Adds column `erloeswert_change_prev_month` to show month-over-month revenue change. |

---

## 5️⃣ Processing Logic
1. Read the CSV files from the input folder.  
2. Write them to the **Bronze** zone (raw data with metadata).  
3. Clean and transform into the **Silver** zone (standardized schema).  
4. Add derived metrics and save to the **Gold** zone.  
5. The final tables are ready for dashboards or further aggregation.

---

## 6️⃣ Example Columns
| Column | Description |
|---------|-------------|
| `Reiseveranstalter` | Tour operator name |
| `Flughafen` | Departure airport |
| `Erlöswert (Tsd EUR)` | Revenue value (thousand EUR) |
| `erloeswert_change_prev_month` | Difference from previous month |
| `ingestion_date` | Date when file was loaded |

---

## 7️⃣ Running and Testing the Application

### ▶ Local Mode (Windows PowerShell)
If you are testing locally on Windows PowerShell, navigate to the project folder and run:

```powershell
# Step 1: Clean and build the assembly JAR
sbt clean assembly

# Step 2: Run the Spark job using the built JAR
spark-submit `
  --class com.eurowingsholidays.ingest.Main `
  target/scala-2.12/eurowings-scala-challenge-assembly-0.1.0.jar
