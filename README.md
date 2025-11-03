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


### ✅ Expected Outcome After successful execution:
 - Your raw data will appear under /bronze/... directories. - Cleaned data will be available in /silver/.... - Final analytical data will be in /gold/..., ready for reporting or dashboards. now it should be ok and it should be proper no duplicate proper as as it is impoetnat my job


## 🚀 Task 2 — Deployment on Databricks

### Overview
The ingestion and transformation pipeline developed in Task 1 can be deployed on **Azure Databricks** to automate and scale the periodic CSV processing delivered by external partners.  
The deployment uses a **CI/CD approach** that ensures reproducibility, consistency, and secure integration with the Azure Data Lake.

---

###  Deployment Architecture
External Partner (CSV Files)  
&emsp;│  
&emsp;▼  
**Azure Data Lake Storage Gen2 (Raw Zone)**  
&emsp;│  
&emsp;▼  
**Databricks Workspace**  
&emsp;├─ Spark Job (Bronze → Silver → Gold)  
&emsp;├─ Delta Lake Tables  
&emsp;└─ Job Monitoring & Logs  
&emsp;│  
&emsp;▼  
**Analytics Layer (Power BI / Synapse)**  
&emsp;▲  
&emsp;│  
**CI/CD Pipeline (GitHub or Azure DevOps)**  

---

### Option 1 – GitHub Actions CI/CD

**Flow:** Developer → GitHub → GitHub Actions → Databricks Repos / Jobs  

1. **Source Control**  
   - All application code (Scala + config) resides in GitHub.  
   - Every commit triggers a GitHub Actions workflow.  

2. **Continuous Integration**  
   - Workflow executes `sbt clean test assembly` to build the JAR.  
   - Unit tests run automatically.  
   - Artifacts are stored as build outputs.  

3. **Continuous Delivery**  
   - Workflow authenticates to Databricks using an access token stored in **GitHub Secrets**.  
   - Steps:  
     - Upload the `.jar` file to **DBFS**.  
     - Create or update a **Databricks Job** that executes the main class (e.g., `VertriebskanalJob`).  
     - Optionally trigger the job run or rely on a schedule.  

4. **Configuration & Security**  
   - Sensitive keys handled through **Databricks Secrets Scope**.  
   - Environment variables defined in the job configuration.  

 **Advantages:** Lightweight, simple integration, minimal maintenance, ideal for small / mid-sized teams.  

---

### Option 2 – Azure DevOps Pipelines CI/CD

**Flow:** Azure Repos / GitHub → Azure Pipelines → Azure Databricks Jobs → ADLS Gen2  

1. **Build Stage**  
   - YAML pipeline runs `sbt clean test assembly`.  
   - Publishes the compiled JAR as an artifact to **Azure Artifacts** or Blob Storage.  

2. **Release Stage**  
   - Deploys the new version to Databricks using the **Databricks CLI** task.  
   - Example YAML snippet:  
     ```yaml
     - task: UsePythonVersion@0
       inputs:
         versionSpec: '3.x'
     - script: |
         databricks jobs deploy --file databricks_job.json
       env:
         DATABRICKS_HOST: $(DATABRICKS_HOST)
         DATABRICKS_TOKEN: $(DATABRICKS_TOKEN)
     ```  
   - Supports approvals and multi-environment promotion (dev → staging → prod).  

3. **Scheduling & Monitoring**  
   - Databricks Jobs or **Azure Data Factory** orchestrate periodic runs.  
   - Logs and metrics are sent to **Azure Monitor** or **Log Analytics**.  

**Advantages:** Enterprise-grade governance, integration with Azure services, RBAC, and centralized monitoring.  

---

###  Result
Both CI/CD options ensure:  
- Automated build, test, and deployment of the Spark job.  
- Secure, parameterized job runs in Databricks.  
- End-to-end traceability from code commit to analytical output.  

---

###  Diagram
Embed or link the deployment diagram here once it’s added to `/docs/task2_deployment_diagram.png`:

![Databricks Deployment Flow](docs/task2_deployment_diagram.png)

---

### Summary
The Spark application is deployed to **Azure Databricks** using either **GitHub Actions** or **Azure DevOps Pipelines**, both implementing continuous integration, testing, artifact packaging, and job deployment to Databricks Jobs.  
This setup provides **scalability, reproducibility, and strong integration** with **Azure Data Lake Storage** and analytical tools.


## 🧱 Task 3 — Making the Application Production-Ready

### Overview
This section outlines the steps and improvements required to make the Spark ingestion pipeline from Task 1 **production-ready**.  
The goal is to ensure reliability, scalability, data quality, security, and cost efficiency — aligned with modern **data lakehouse architecture** standards used in the airline and travel analytics industry.

---

### 1. Data Lakehouse Design
Adopt a **Delta Lake-based Lakehouse** model to unify batch and streaming processing while maintaining data quality.

- Organize data in **Bronze → Silver → Gold** layers for governance and clarity.  
- Use **Delta format** for ACID transactions, schema evolution, and time travel.  
- Partition data by `ingestion_date` and `dataset_name` for optimized querying.  

**Alternative:**  
For multi-cloud or open-source setups, **Apache Iceberg** or **Hudi** can be used instead of Delta.

---

### 2. Schema and Data Modeling
Implement a **Star Schema** in the Gold layer to improve analytical performance and usability.

- **Fact table:** Booking or Sales metrics (e.g., `Erlöswert`, `Personenanzahl`).  
- **Dimension tables:** Time, Product, Airport, Reiseveranstalter, etc.  
- This structure supports easy integration with BI tools such as **Power BI** or **Synapse**.

**Alternative:**  
Use a **Wide Table** approach during data exploration or prototyping; later transition to a Star Schema for production analytics.

---

### 3. Orchestration and Automation
Use **Databricks Workflows** or **Azure Data Factory** for end-to-end orchestration.

- Automatically detect new CSV files in ADLS.  
- Trigger Databricks Jobs on schedule or event-based (e.g., monthly deliveries).  
- Send alerts to Teams/Slack upon completion or failure.

**Alternative:**  
For near real-time ingestion, use **Autoloader (Cloud Files)** with **Event Grid** triggers.

---

### 4. Data Quality and Validation
Ensure consistent and reliable data using automated validation frameworks.

- Integrate **Deequ** or **Great Expectations** for schema, null checks, and anomaly detection.  
- Record validation results into Delta tables for traceability.  
- Fail or quarantine data when critical quality checks fail.

---

### 5. Observability and Logging
Improve visibility of data jobs and enable fast troubleshooting.

- Use **Databricks Job Run Dashboard** and **Azure Log Analytics** for central monitoring.  
- Add custom Spark metrics via **Dropwizard** or **Prometheus** exporters.  
- Maintain audit fields such as `created_at`, `source_file`, and `run_id`.

---

### 6. CI/CD and Environment Strategy
Automate the build → test → deploy cycle using **GitHub Actions** or **Azure Pipelines**.

- Promote code through environments (`dev → staging → prod`).  
- Parameterize configuration using Databricks Secrets and environment variables.  
- Add **unit tests** (via `spark-testing-base`) and **integration tests** (via small sample data).  

---

### 7. Security and Compliance
Ensure compliance with GDPR and enterprise security standards.

- Use **Azure Managed Identities** and **Key Vault** for secure credential handling.  
- Restrict ADLS and Databricks access via **RBAC**.  
- Encrypt data at rest and in transit (TLS/SSL).  
- Apply retention and audit policies using **Delta Time Travel** and **VACUUM**.

---

### 8. Performance and Cost Optimization
Optimize performance and reduce compute costs in Databricks.

- Enable **Auto-Optimize**, **Z-Ordering**, and **Caching** for frequent queries.  
- Use **Cluster Pools** and **Job Clusters** to avoid idle compute.  
- Compress intermediate data using **Parquet + Snappy**.  

---

### ✅ Summary
The recommended production setup is a **Delta Lakehouse with a Star Schema Gold layer**, orchestrated by **Databricks Workflows** and deployed via **CI/CD pipelines** (GitHub or Azure DevOps).  
This architecture ensures **data integrity, scalability, cost-efficiency, and analytical performance** while maintaining governance and observability across the entire data platform.
