# Databricks notebook source
# MAGIC %md
# MAGIC # Findings, Limitations, and Responsible-Use Notes
# MAGIC
# MAGIC This notebook summarizes the main findings, architecture, limitations, and responsible-use boundaries of the Synthetic Patient Utilization Intelligence project.
# MAGIC
# MAGIC The project uses only Synthea-generated synthetic healthcare records. It demonstrates healthcare data engineering, utilization analytics, and system design in Databricks. It is not a clinical, financial, or operational decision-support system.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_patients,
# MAGIC     SUM(total_encounters) AS total_encounters,
# MAGIC     ROUND(AVG(total_encounters), 2) AS average_encounters_per_patient,
# MAGIC     SUM(emergency_encounters) AS emergency_encounters,
# MAGIC     SUM(inpatient_encounters) AS inpatient_encounters,
# MAGIC     SUM(repeat_encounters_within_30d) AS repeat_encounters_within_30d,
# MAGIC     ROUND(SUM(total_claim_cost), 2) AS total_recorded_claim_cost
# MAGIC FROM workspace.default.gold_patient_utilization;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Findings
# MAGIC
# MAGIC 1. **Utilization was highly concentrated.** The dataset contained 7,210 encounters across 113 synthetic patients, averaging 63.81 encounters per patient. A smaller number of patients recorded substantially higher utilization than the overall average.
# MAGIC
# MAGIC 2. **Older synthetic patients generally recorded greater utilization.** The 65+ group had the highest average number of encounters, while the under-18 and 18–34 groups had lower averages.
# MAGIC
# MAGIC 3. **Ambulatory care was the largest encounter category.** Outpatient, wellness, urgent-care, emergency, inpatient, home, hospice, and virtual encounters appeared at lower volumes.
# MAGIC
# MAGIC 4. **Encounter volume and recorded claim cost were positively associated, but not perfectly aligned.** Patients with similar encounter counts sometimes had different cost totals, suggesting that encounter type and service mix also influenced the synthetic costs.
# MAGIC
# MAGIC 5. **High-utilization records contained more than diagnoses.** The condition data included disorders, social findings, employment information, medication-review situations, and environmental observations. Healthcare systems must distinguish among these concept types rather than treating every coded record as a disease.

# COMMAND ----------

# MAGIC %md
# MAGIC ## System Architecture
# MAGIC
# MAGIC The project follows a Bronze–Silver–Gold medallion architecture:
# MAGIC
# MAGIC - **Raw source:** Synthea CSV files stored in a governed Unity Catalog volume.
# MAGIC - **Bronze layer:** Eight Delta tables preserving original source values and adding ingestion metadata.
# MAGIC - **Silver layer:** Eight cleaned and standardized tables covering patients, encounters, conditions, medications, procedures, observations, providers, and organizations.
# MAGIC - **Gold layer:** A patient-level utilization table combining encounter, cost, condition, medication, procedure, provider, and organization features.
# MAGIC - **Presentation layer:** SQL analytics, notebook visualizations, and an AI/BI dashboard for healthcare-utilization review.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limitations
# MAGIC
# MAGIC - The data is synthetic and does not represent the utilization patterns of a real healthcare population.
# MAGIC - Results should not be generalized to a real hospital, insurer, health system, or geographic population.
# MAGIC - Recorded claim costs are synthetic and are not validated billing, reimbursement, or accounting figures.
# MAGIC - A repeat encounter within 30 days is an operational timing feature, not a validated hospital-readmission measure.
# MAGIC - The high-utilization threshold is descriptive and should not be interpreted as a clinical-risk classification.
# MAGIC - Condition records include disorders, findings, situations, and social-context concepts; they are not all diagnoses.
# MAGIC - The analysis identifies associations, not causal relationships.
# MAGIC - The dataset contains only 113 synthetic patients and is suitable for prototyping rather than production benchmarking.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Privacy and Responsible-Use Boundaries
# MAGIC
# MAGIC - No real patient data was used.
# MAGIC - Direct identifier-like fields—including names, synthetic Social Security numbers, passport values, driver's-license values, and street addresses—were excluded from analytical Silver and Gold tables.
# MAGIC - The dashboard uses synthetic patient identifiers rather than names.
# MAGIC - The project does not diagnose conditions, recommend treatment, prioritize real patients, or automate healthcare decisions.
# MAGIC - Any real-world adaptation would require formal data governance, security review, privacy protections, bias evaluation, stakeholder validation, and human oversight.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Future Work
# MAGIC
# MAGIC - Scale the pipeline to a larger Synthea population.
# MAGIC - Add automated data-quality expectations and pipeline monitoring.
# MAGIC - Add date, age-group, encounter-class, and organization filters to the dashboard.
# MAGIC - Evaluate utilization across consistent historical observation windows.
# MAGIC - Add MLflow tracking for a clearly non-clinical utilization-modeling baseline.
# MAGIC - Use time-based validation rather than random row splitting for future predictive experiments.
# MAGIC - Conduct stakeholder discovery with healthcare analysts before defining production requirements.

# COMMAND ----------
