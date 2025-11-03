# Eurowings Scala Challenge

**Author:** Davinder Kaur  
**Date:** November 2025  
**Technologies:** Scala, Apache Spark, Azure Databricks, Azure DevOps

##  Task 1 – Spark Data Ingestion and Preparation

### Overview
This Scala/Spark application reads CSV files from an external partner (e.g. `Vertriebskanal_20250314.csv`), cleans the data, and stores it in a Data Lake for reporting.  
Each delivery is saved by dataset name and load date, making it easy to trace and validate new files.

---

### Approach
The pipeline follows a simple **Bronze → Silver → Gold** structure:

- **Bronze:** Raw CSVs are stored exactly as received but validated for correct file type (`.csv`) and encoding.  
  Invalid or empty files are skipped and logged.  
- **Silver:** Data is cleaned — column names are standardized, unnecessary spaces and special characters removed, and types are cast to numeric or string as needed.  
  Missing or invalid values (e.g. empty `Erlöswert`) are filled with defaults or flagged for review.  
- **Gold:** Adds the derived field `erloeswert_change_prev_month` to show month-over-month change in revenue.

---

### Data Parameters and Validation
- Checked columns: `Reiseveranstalter`, `Flughafen`, `Erlöswert (Tsd EUR)`, `Personenanzahl`, etc.  
- Controlled string length for text fields to avoid downstream issues.  
- Ensured numeric columns (`Erlöswert`, `Personenanzahl`) are valid decimals — no special characters.  
- Handled fractional and negative values (as per business note: fractional = expected, negative = cancellations).  
- Ensured schema consistency across files using Spark schema inference and `.option("enforceSchema", true)`.

If more metadata were provided (e.g. region, currency, or booking source), we could extend the schema to enrich reporting and build better KPIs.

---

### Tools
Scala 2.12 • Spark 3.5 • sbt • Parquet/Delta • Config file (`application.conf`)

---

###  Run Locally
You can run the project manually or using a helper script.

#### Option 1 – Direct Commands
```
sbt clean assembly
spark-submit --class com.eurowingsholidays.ingest.Main target/scala-2.12/eurowings-scala-challenge-assembly-0.1.0.jar
```
#### Option 2 – Using a Script

Create a file called run.sh:

```bash
#!/bin/bash
sbt clean assembly
spark-submit --class com.eurowingsholidays.ingest.Main \
  target/scala-2.12/eurowings-scala-challenge-assembly-0.1.0.jar
```
Give it permission to execute:

```bash
chmod 400 run.sh
chmod +x run.sh
./run.sh
```

This makes the project runnable on all platforms (Windows via Git Bash or Linux/Mac terminal).

#### Notes on Data and Improvements

- The sample data lacked some metadata like booking channels, product type, or time granularity — adding these would improve trend analysis.

- With historical data over multiple years, we could build seasonality models or cancellation trends.

- If the source provided unique booking IDs, we could also validate duplicates and detect anomalies in reporting.

##### Output Summary

 - **/bronze:** Raw CSVs with ingestion date partitions.
 - **/silver:** Cleaned and standardized datasets.
 - **/gold:** Final analytics-ready data with derived metrics.




##  Task 2 – Deployment on Databricks

### Overview
The Spark job is deployed on **Azure Databricks** to process new CSV deliveries automatically.  
Code and jobs are managed through **Azure DevOps CI/CD**, which builds, tests, and deploys the application  
across different environments — Development, Test, and Production.  
Nightly builds update the Test environment automatically, while Production releases require manual approval  
with secure parameters, permissions, and Key Vault secrets.

---

### **Data Flow**

External Partner (CSV Files)  
&emsp;│  
&emsp;▼  
**Azure Data Lake Gen2 (Raw Zone)**  
&emsp;│  
&emsp;▼  
**Databricks Workspace**  
&emsp;├─ Spark Job (Bronze → Silver → Gold)  
&emsp;├─ Delta Tables / Logs  
&emsp;└─ Monitoring → Azure Log Analytics / Grafana  
&emsp;│  
&emsp;▼  
**Analytics Layer (Power BI / Synapse)**  

---

### **CI/CD Flow**

Developer (Feature Branch)  
&emsp;│  
&emsp;▼  
PR → Merge to **Main** (after code review)  
&emsp;│  
&emsp;▼  
**Build Pipeline**  
 ├ `sbt test` – run unit tests  
 ├ `sbt clean assembly` – build application JAR  
 └ Publish artifact to Azure Artifacts  
&emsp;│  
&emsp;▼  
  **Nightly deploy** from **Main → Test** (automatic)  
&emsp;│  
&emsp;▼  
  **Manual Approval → Deploy to Prod**

The Production release pipeline is triggered **manually** after validation and approval.  
Once started, it runs automatically — deploying the same artifact used in Test to the **Prod Databricks workspace**.  
The pipeline also generates and publishes **release documentation** (build version, change summary, configuration notes)  
automatically to the project Wiki or release page in Azure DevOps.

After deployment, a **manual four-eyes check** is performed to verify:
  
- Key Vault secrets and environment-specific settings.  
- Connection parameters, permissions, and access configurations.  
- That all expected Databricks jobs, clusters, and data outputs are running correctly.  

This ensures a secure, validated, and fully traceable production deployment with both automation and human oversight.

---

This flow ensures a controlled, auditable release process:  
- Developers work safely on feature branches.  
- Code is tested and validated nightly in Test.  
- Production releases are approved, documented, and verified before go-live.


**Highlights**
- Automatic nightly deploys to *Test* for validation.  
- Manual approvals before *Prod* release.  
- Logs and metrics sent to Azure Monitor / Log Analytics  or Grafana.  
- Safe, auditable, and repeatable pipeline for Databricks jobs.

---

##  Task 3 – Making It Production-Ready

### Overview
To make the pipeline stable and enterprise-ready, a few key improvements are required.

---

### Key Improvements
- Use **Delta Lake** for ACID updates, schema evolution, and history.  
- Build a **Star Schema** in the Gold layer (fact + dimensions).  
- Orchestrate and schedule jobs through **Databricks Workflows**, triggered automatically by the **Azure DevOps pipeline** for Test and manually for Prod releases.  
- Add **data-quality checks** (Great Expectations / Deequ).  
- Centralize **monitoring and alerts** in Azure Log Analytics.  
- Secure access via **Key Vault**, RBAC, and encryption.  
- Improve performance with **Auto-Optimize**, caching, and cluster pools.

---

Access to Databricks jobs can be restricted per user or group.  
Each pipeline (job) has its own permissions, so only authorized users can view or run their assigned workflows — ensuring data isolation and secure collaboration.

---
### Summary
A **Delta Lakehouse** with **Bronze/Silver/Gold layers** and a **Star Schema** in Gold delivers clean, reliable, and scalable data.  
Deployed through **Azure DevOps CI/CD** with nightly testing and controlled production releases, it ensures automation, quality, and easy analytics integration.  
Together, these improvements make the pipeline robust, secure, and fully production-ready for automated operation in the Databricks environment.
