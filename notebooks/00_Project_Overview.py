# Databricks notebook source
# MAGIC %md
# MAGIC # Synthetic Patient Utilization Intelligence
# MAGIC
# MAGIC Healthcare information is often distributed across separate patient, encounter, condition, medication, procedure, observation, provider, organization, and cost records. This project builds a reproducible Databricks pipeline that transforms synthetic healthcare data into patient-level utilization insights for health-informatics and care-management review.
# MAGIC
# MAGIC Using Synthea-generated patient records, the project will:
# MAGIC
# MAGIC * ingest and validate multiple healthcare datasets;
# MAGIC * organize data into raw, cleaned, and analysis-ready layers;
# MAGIC * calculate patient-level emergency, inpatient, condition, medication, and cost features;
# MAGIC * analyze repeated-utilization and healthcare-operations patterns;
# MAGIC * present findings through SQL queries and an interactive dashboard; and
# MAGIC * document responsible-use boundaries, limitations, and future work.
# MAGIC
# MAGIC This project uses only synthetic patient records. It is intended for education, healthcare analytics, and system-design practice. It does not predict individual health outcomes, recommend treatment, or provide clinical decision support.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   current_catalog() AS catalog_name,
# MAGIC   current_schema() AS schema_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.synthea_raw
# MAGIC COMMENT 'Raw Synthea CSV files for the Synthetic Patient Utilization Intelligence project';

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN workspace.default;
