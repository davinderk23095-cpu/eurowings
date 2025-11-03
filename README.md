# Eurowings Scala Challenge

## Task 1 – Spark Data Ingestion and Preparation

### Overview
This Scala/Spark app reads CSV files from an external partner (e.g. `Vertriebskanal_20250314.csv`), cleans them, and saves them into a Data Lake for reporting.  
Each file is stored by date and dataset name, so every delivery can be traced easily.

### Approach
The pipeline follows a simple **Bronze → Silver → Gold** setup:
- **Bronze:** Raw data stored as received.
- **Silver:** Cleaned and formatted data.
- **Gold:** Final table with a new column showing monthly change in *Erlöswert*.

### Tools
Scala 2.12 • Spark 3.5 • sbt • Parquet/Delta • Config file (`application.conf`)

### Run Locally
```bash
sbt clean assembly
spark-submit --class com.eurowingsholidays.ingest.Main target/scala-2.12/eurowings-scala-challenge-assembly-0.1.0.jar

##  Task 2 – Deployment on Databricks

### Overview
The Spark job can be deployed on **Azure Databricks** to process new CSV deliveries automatically.  
Code and jobs are managed through **Azure DevOps CI/CD**, which builds, tests, and deploys the job to different environments with approval control.

---

###  Databricks Deployment Flow (Azure DevOps – Realistic Setup)

**Branching & Environments**
- **Personal branches:** each developer works freely.  
- **Development branch:** integrates personal work and runs tests.  
- **Main branch:** reviewed and stable; used for nightly deployments.  
- **Test:** environment updated nightly from *main*.  
- **Prod:** manual approval required before release.

---

**Data Flow**

External Partner (CSV Files)  
&emsp;│  
&emsp;▼  
**Azure Data Lake Gen2 (Raw Zone)**  
&emsp;│  
&emsp;▼  
**Databricks Workspace**  
&emsp;├─ Spark Job (Bronze → Silver → Gold)  
&emsp;├─ Delta Tables / Logs  
&emsp;└─ Monitoring → Azure Log Analytics  
&emsp;│  
&emsp;▼  
**Analytics Layer (Power BI / Synapse)**  

---

**CI/CD Flow**

Developer (Personal Branch)  
&emsp;│  
&emsp;▼  
PR → Merge to **Development**  
&emsp;│  
&emsp;▼  
 Build Pipeline  
 ├ `sbt test`  
 ├ `sbt clean assembly`  
 └ Publish artifact to Azure Artifacts  
&emsp;│  
&emsp;▼  
  Nightly deploy from **Main** → **Test**  
&emsp;│  
&emsp;▼  
 Manual approval → Deploy to **Prod** (Databricks CLI / API)  

---

**Highlights**
- Automatic nightly deploys to *Test* for validation.  
- Manual approvals before *Prod* release.  
- Logs and metrics sent to Azure Monitor / Log Analytics.  
- Safe, auditable, and repeatable pipeline for Databricks jobs.

![Databricks DevOps Deployment Flow](docs/task2_deployment_diagram.png)

---

##  Task 3 – Making It Production-Ready

### Overview
To make the pipeline stable and enterprise-ready, a few key improvements are required.

---

### Key Improvements
- Use **Delta Lake** for ACID updates, schema evolution, and history.  
- Build a **Star Schema** in the Gold layer (fact + dimensions).  
- Automate job triggers with **Databricks Workflows** or **ADF**.  
- Add **data quality checks** (Great Expectations / Deequ).  
- Centralize **monitoring and alerts** in Azure Log Analytics.  
- Secure access via **Key Vault**, RBAC, and encryption.  
- Improve performance with **Auto-Optimize**, caching, and cluster pools.

---

### Summary
A **Delta Lakehouse** with **Bronze/Silver/Gold layers** and a **Star Schema** in Gold delivers clean, reliable, and scalable data.  
Deployed through **Azure DevOps CI/CD** with nightly testing and controlled production releases, it ensures automation, quality, and easy analytics integration.
