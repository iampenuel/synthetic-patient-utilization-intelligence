# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Patient Utilization Table
# MAGIC
# MAGIC This notebook combines cleaned Silver tables into one patient-level analytical dataset.
# MAGIC
# MAGIC Each row represents one synthetic patient and summarizes:
# MAGIC
# MAGIC - demographic and geographic characteristics;
# MAGIC - emergency, inpatient, outpatient, and total encounter activity;
# MAGIC - healthcare costs;
# MAGIC - conditions, medications, and procedures;
# MAGIC - organizations and providers involved in care;
# MAGIC - recent and repeated utilization patterns; and
# MAGIC - the time since the patient’s most recent recorded encounter.
# MAGIC
# MAGIC The resulting Gold table is designed for healthcare-operations and utilization analysis. It does not provide diagnoses, treatment recommendations, or validated clinical risk predictions.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

patients_df = spark.table(
    "workspace.default.silver_patients"
)

encounters_df = spark.table(
    "workspace.default.silver_encounters"
)

conditions_df = spark.table(
    "workspace.default.silver_conditions"
)

medications_df = spark.table(
    "workspace.default.silver_medications"
)

procedures_df = spark.table(
    "workspace.default.silver_procedures"
)

print(f"Patients: {patients_df.count():,}")
print(f"Encounters: {encounters_df.count():,}")
print(f"Conditions: {conditions_df.count():,}")
print(f"Medications: {medications_df.count():,}")
print(f"Procedures: {procedures_df.count():,}")

# COMMAND ----------

reference_date = (
    encounters_df
    .select(
        F.max(
            F.to_date("start_time")
        ).alias("reference_date")
    )
    .first()["reference_date"]
)

if reference_date is None:
    raise ValueError(
        "No valid encounter dates were found."
    )

print(f"Dataset reference date: {reference_date}")

# COMMAND ----------

display(
    encounters_df
    .groupBy("encounter_class")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

encounter_window = (
    Window
    .partitionBy("patient_id")
    .orderBy("start_time")
)

encounter_sequence_df = (
    encounters_df
    .select(
        "encounter_id",
        "patient_id",
        "start_time",
        "encounter_class",
        "organization_id",
        "provider_id",
        "total_claim_cost",
    )
    .withColumn(
        "previous_encounter_time",
        F.lag("start_time").over(encounter_window),
    )
    .withColumn(
        "days_since_previous_encounter",
        F.datediff(
            F.to_date("start_time"),
            F.to_date("previous_encounter_time"),
        ),
    )
)

encounter_summary_df = (
    encounter_sequence_df
    .groupBy("patient_id")
    .agg(
        F.countDistinct("encounter_id")
            .alias("total_encounters"),

        F.sum(
            F.when(
                F.col("encounter_class") == "emergency",
                1,
            ).otherwise(0)
        ).alias("emergency_encounters"),

        F.sum(
            F.when(
                F.col("encounter_class") == "inpatient",
                1,
            ).otherwise(0)
        ).alias("inpatient_encounters"),

        F.sum(
            F.when(
                F.col("encounter_class") == "outpatient",
                1,
            ).otherwise(0)
        ).alias("outpatient_encounters"),

        F.sum(
            F.when(
                F.col("encounter_class") == "ambulatory",
                1,
            ).otherwise(0)
        ).alias("ambulatory_encounters"),

        F.sum(
            F.when(
                F.col("encounter_class") == "urgentcare",
                1,
            ).otherwise(0)
        ).alias("urgent_care_encounters"),

        F.sum(
            F.when(
                (
                    F.col("days_since_previous_encounter") >= 1
                )
                & (
                    F.col("days_since_previous_encounter") <= 30
                ),
                1,
            ).otherwise(0)
        ).alias("repeat_encounters_within_30d"),

        F.round(
            F.sum("total_claim_cost"),
            2,
        ).alias("total_claim_cost"),

        F.round(
            F.avg("total_claim_cost"),
            2,
        ).alias("average_claim_cost"),

        F.min(
            F.to_date("start_time")
        ).alias("first_encounter_date"),

        F.max(
            F.to_date("start_time")
        ).alias("last_encounter_date"),

        F.countDistinct("organization_id")
            .alias("organizations_visited"),

        F.countDistinct("provider_id")
            .alias("providers_seen"),
    )
)

# COMMAND ----------

condition_summary_df = (
    conditions_df
    .groupBy("patient_id")
    .agg(
        F.count("*")
            .alias("condition_record_count"),

        F.countDistinct("condition_code")
            .alias("distinct_condition_count"),

        F.sum(
            F.when(
                F.col("is_active") == True,
                1,
            ).otherwise(0)
        ).alias("active_condition_count"),
    )
)


medication_summary_df = (
    medications_df
    .groupBy("patient_id")
    .agg(
        F.count("*")
            .alias("medication_record_count"),

        F.countDistinct("medication_code")
            .alias("distinct_medication_count"),

        F.sum(
            F.when(
                F.col("is_active") == True,
                1,
            ).otherwise(0)
        ).alias("active_medication_count"),

        F.round(
            F.sum("total_cost"),
            2,
        ).alias("total_medication_cost"),
    )
)


procedure_summary_df = (
    procedures_df
    .groupBy("patient_id")
    .agg(
        F.count("*")
            .alias("procedure_record_count"),

        F.countDistinct("procedure_code")
            .alias("distinct_procedure_count"),

        F.round(
            F.sum("base_cost"),
            2,
        ).alias("total_procedure_cost"),
    )
)

# COMMAND ----------

gold_patient_utilization_df = (
    patients_df
    .select(
        "patient_id",
        "birth_date",
        "death_date",
        "is_deceased",
        "gender",
        "race",
        "ethnicity",
        "state",
        "county",
        "lifetime_healthcare_expenses",
        "lifetime_healthcare_coverage",
        "income",
    )
    .withColumn(
        "age_reference_date",
        F.when(
            F.col("is_deceased"),
            F.col("death_date"),
        ).otherwise(
            F.lit(reference_date)
        ),
    )
    .withColumn(
        "age",
        F.floor(
            F.months_between(
                F.col("age_reference_date"),
                F.col("birth_date"),
            ) / 12
        ),
    )
    .withColumn(
        "age_group",
        F.when(
            F.col("age") < 18,
            "Under 18",
        )
        .when(
            F.col("age") < 35,
            "18-34",
        )
        .when(
            F.col("age") < 50,
            "35-49",
        )
        .when(
            F.col("age") < 65,
            "50-64",
        )
        .otherwise("65+"),
    )
    .join(
        encounter_summary_df,
        on="patient_id",
        how="left",
    )
    .join(
        condition_summary_df,
        on="patient_id",
        how="left",
    )
    .join(
        medication_summary_df,
        on="patient_id",
        how="left",
    )
    .join(
        procedure_summary_df,
        on="patient_id",
        how="left",
    )
    .withColumn(
        "days_since_last_encounter",
        F.datediff(
            F.lit(reference_date),
            F.col("last_encounter_date"),
        ),
    )
)

# COMMAND ----------

zero_fill_columns = [
    "total_encounters",
    "emergency_encounters",
    "inpatient_encounters",
    "outpatient_encounters",
    "ambulatory_encounters",
    "urgent_care_encounters",
    "repeat_encounters_within_30d",
    "total_claim_cost",
    "average_claim_cost",
    "organizations_visited",
    "providers_seen",
    "condition_record_count",
    "distinct_condition_count",
    "active_condition_count",
    "medication_record_count",
    "distinct_medication_count",
    "active_medication_count",
    "total_medication_cost",
    "procedure_record_count",
    "distinct_procedure_count",
    "total_procedure_cost",
]

gold_patient_utilization_df = (
    gold_patient_utilization_df
    .fillna(
        0,
        subset=zero_fill_columns,
    )
    .withColumn(
        "_gold_updated_at",
        F.current_timestamp(),
    )
)

# COMMAND ----------

gold_table_name = (
    "workspace.default."
    "gold_patient_utilization"
)

(
    gold_patient_utilization_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_table_name)
)

print(
    f"Created Gold table: {gold_table_name}"
)

# COMMAND ----------

gold_df = spark.table(
    "workspace.default.gold_patient_utilization"
)

gold_validation = (
    gold_df
    .agg(
        F.count("*").alias("total_rows"),
        F.count("patient_id").alias("non_null_patient_ids"),
        F.countDistinct("patient_id").alias("distinct_patient_ids"),
        F.min("age").alias("minimum_age"),
        F.max("age").alias("maximum_age"),
        F.sum("total_encounters").alias("all_encounters"),
    )
)

display(gold_validation)

# COMMAND ----------

validation_row = gold_validation.first()

assert validation_row["total_rows"] == 113, (
    "Gold table should contain one row for each of the 113 patients."
)

assert (
    validation_row["total_rows"]
    == validation_row["non_null_patient_ids"]
    == validation_row["distinct_patient_ids"]
), "Gold patient IDs are missing or duplicated."

assert validation_row["all_encounters"] == 7210, (
    "Gold encounter totals do not match the Silver encounter table."
)

print("GOLD QUALITY GATE: PASS")

# COMMAND ----------

display(
    gold_df
    .select(
        "patient_id",
        "age",
        "age_group",
        "total_encounters",
        "emergency_encounters",
        "inpatient_encounters",
        "repeat_encounters_within_30d",
        "distinct_condition_count",
        "distinct_medication_count",
        "procedure_record_count",
        "total_claim_cost",
        "days_since_last_encounter",
    )
    .orderBy(
        F.desc("total_encounters")
    )
    .limit(20)
)

# COMMAND ----------

from pyspark.sql import functions as F

gold_df = spark.table(
    "workspace.default.gold_patient_utilization"
)

gold_check = gold_df.agg(
    F.count("*").alias("total_rows"),
    F.count("patient_id").alias("non_null_patient_ids"),
    F.countDistinct("patient_id").alias("distinct_patient_ids"),
    F.sum("total_encounters").alias("all_encounters"),
    F.min("age").alias("minimum_age"),
    F.max("age").alias("maximum_age"),
).first()

assert gold_check["total_rows"] == 113
assert gold_check["non_null_patient_ids"] == 113
assert gold_check["distinct_patient_ids"] == 113
assert gold_check["all_encounters"] == 7210
assert gold_check["minimum_age"] >= 0

print("GOLD QUALITY GATE: PASS")
print(gold_check.asDict())

# COMMAND ----------
