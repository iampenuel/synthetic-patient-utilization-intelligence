# Databricks notebook source
# MAGIC %md
# MAGIC # Patient Utilization Analytics
# MAGIC
# MAGIC This notebook analyzes the Gold patient-utilization table and supporting Silver datasets to identify healthcare-utilization patterns across synthetic patients.
# MAGIC
# MAGIC The analysis focuses on operational questions such as:
# MAGIC
# MAGIC - How is utilization distributed across age groups?
# MAGIC - Which encounter categories appear most frequently?
# MAGIC - Which patients have the highest recorded utilization and costs?
# MAGIC - Which conditions appear most often among high-utilization patients?
# MAGIC - How strongly are encounter volume and claim costs related?
# MAGIC
# MAGIC All records are synthetic. The findings are educational and operational rather than clinical.

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

# MAGIC %sql
# MAGIC SELECT
# MAGIC     age_group,
# MAGIC     COUNT(*) AS patient_count,
# MAGIC     ROUND(AVG(total_encounters), 2) AS average_encounters,
# MAGIC     ROUND(AVG(emergency_encounters), 2) AS average_emergency_encounters,
# MAGIC     ROUND(AVG(inpatient_encounters), 2) AS average_inpatient_encounters,
# MAGIC     ROUND(AVG(total_claim_cost), 2) AS average_claim_cost
# MAGIC FROM workspace.default.gold_patient_utilization
# MAGIC GROUP BY age_group
# MAGIC ORDER BY
# MAGIC     CASE age_group
# MAGIC         WHEN 'Under 18' THEN 1
# MAGIC         WHEN '18-34' THEN 2
# MAGIC         WHEN '35-49' THEN 3
# MAGIC         WHEN '50-64' THEN 4
# MAGIC         WHEN '65+' THEN 5
# MAGIC     END;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     encounter_class,
# MAGIC     COUNT(*) AS encounter_count,
# MAGIC     ROUND(
# MAGIC         COUNT(*) * 100.0 /
# MAGIC         SUM(COUNT(*)) OVER (),
# MAGIC         2
# MAGIC     ) AS percentage_of_encounters
# MAGIC FROM workspace.default.silver_encounters
# MAGIC GROUP BY encounter_class
# MAGIC ORDER BY encounter_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     patient_id,
# MAGIC     age_group,
# MAGIC     total_encounters,
# MAGIC     total_claim_cost
# MAGIC FROM workspace.default.gold_patient_utilization
# MAGIC WHERE total_encounters > 0
# MAGIC   AND total_claim_cost IS NOT NULL
# MAGIC ORDER BY total_encounters DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     patient_id,
# MAGIC     age,
# MAGIC     age_group,
# MAGIC     total_encounters,
# MAGIC     emergency_encounters,
# MAGIC     inpatient_encounters,
# MAGIC     repeat_encounters_within_30d,
# MAGIC     distinct_condition_count,
# MAGIC     distinct_medication_count,
# MAGIC     procedure_record_count,
# MAGIC     ROUND(total_claim_cost, 2) AS total_claim_cost
# MAGIC FROM workspace.default.gold_patient_utilization
# MAGIC ORDER BY total_encounters DESC
# MAGIC LIMIT 15;

# COMMAND ----------
# MAGIC %sql
# MAGIC WITH utilization_threshold AS (
# MAGIC     SELECT
# MAGIC         percentile_approx(total_encounters, 0.90) AS p90_encounters
# MAGIC     FROM workspace.default.gold_patient_utilization
# MAGIC ),
# MAGIC
# MAGIC high_utilizers AS (
# MAGIC     SELECT
# MAGIC         patient_id
# MAGIC     FROM workspace.default.gold_patient_utilization
# MAGIC     CROSS JOIN utilization_threshold
# MAGIC     WHERE total_encounters >= p90_encounters
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     c.condition_description,
# MAGIC     COUNT(DISTINCT c.patient_id) AS affected_high_utilizers,
# MAGIC     COUNT(*) AS condition_records
# MAGIC FROM workspace.default.silver_conditions AS c
# MAGIC INNER JOIN high_utilizers AS h
# MAGIC     ON c.patient_id = h.patient_id
# MAGIC WHERE c.condition_description IS NOT NULL
# MAGIC GROUP BY c.condition_description
# MAGIC ORDER BY
# MAGIC     affected_high_utilizers DESC,
# MAGIC     condition_records DESC
# MAGIC LIMIT 15;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COALESCE(
# MAGIC         o.organization_name,
# MAGIC         'Unknown organization'
# MAGIC     ) AS organization_name,
# MAGIC     COUNT(DISTINCT e.encounter_id) AS encounter_count,
# MAGIC     COUNT(DISTINCT e.patient_id) AS patient_count,
# MAGIC     ROUND(SUM(e.total_claim_cost), 2) AS total_claim_cost
# MAGIC FROM workspace.default.silver_encounters AS e
# MAGIC LEFT JOIN workspace.default.silver_organizations AS o
# MAGIC     ON e.organization_id = o.organization_id
# MAGIC GROUP BY
# MAGIC     COALESCE(
# MAGIC         o.organization_name,
# MAGIC         'Unknown organization'
# MAGIC     )
# MAGIC ORDER BY encounter_count DESC
# MAGIC LIMIT 15;

# COMMAND ----------
