# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Data Cleaning and Standardization
# MAGIC
# MAGIC This notebook transforms raw Synthea Bronze tables into cleaned and analysis-ready Silver tables.
# MAGIC
# MAGIC The Silver layer will:
# MAGIC
# MAGIC - standardize column names;
# MAGIC - convert dates, timestamps, and numeric values into appropriate data types;
# MAGIC - remove duplicate records;
# MAGIC - validate identifiers and table relationships;
# MAGIC - calculate basic encounter features;
# MAGIC - exclude unnecessary direct identifiers from analytical tables; and
# MAGIC - preserve traceability to the source Bronze records.

# COMMAND ----------

from pyspark.sql import functions as F

patients_bronze_df = spark.table(
    "workspace.default.bronze_patients"
)

required_patient_columns = [
    "Id",
    "BIRTHDATE",
    "DEATHDATE",
    "GENDER",
    "RACE",
    "ETHNICITY",
    "STATE",
    "COUNTY",
    "ZIP",
    "LAT",
    "LON",
    "HEALTHCARE_EXPENSES",
    "HEALTHCARE_COVERAGE",
    "INCOME",
]

missing_patient_columns = sorted(
    set(required_patient_columns)
    - set(patients_bronze_df.columns)
)

if missing_patient_columns:
    raise ValueError(
        f"Missing patient columns: {missing_patient_columns}"
    )

silver_patients_df = (
    patients_bronze_df
    .select(
        F.col("Id").alias("patient_id"),
        F.to_date("BIRTHDATE").alias("birth_date"),
        F.to_date("DEATHDATE").alias("death_date"),
        F.upper(F.trim("GENDER")).alias("gender"),
        F.initcap(F.trim("RACE")).alias("race"),
        F.initcap(F.trim("ETHNICITY")).alias("ethnicity"),
        F.upper(F.trim("STATE")).alias("state"),
        F.trim("COUNTY").alias("county"),
        F.trim("ZIP").alias("zip_code"),
        F.col("LAT").cast("double").alias("latitude"),
        F.col("LON").cast("double").alias("longitude"),
        F.col("HEALTHCARE_EXPENSES")
            .cast("double")
            .alias("lifetime_healthcare_expenses"),
        F.col("HEALTHCARE_COVERAGE")
            .cast("double")
            .alias("lifetime_healthcare_coverage"),
        F.col("INCOME")
            .cast("double")
            .alias("income"),
    )
    .dropDuplicates(["patient_id"])
    .withColumn(
        "is_deceased",
        F.col("death_date").isNotNull(),
    )
    .withColumn(
        "_silver_updated_at",
        F.current_timestamp(),
    )
)

(
    silver_patients_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.silver_patients")
)

display(silver_patients_df.limit(10))

# COMMAND ----------

encounters_bronze_df = spark.table(
    "workspace.default.bronze_encounters"
)

required_encounter_columns = [
    "Id",
    "START",
    "STOP",
    "PATIENT",
    "ORGANIZATION",
    "PROVIDER",
    "PAYER",
    "ENCOUNTERCLASS",
    "CODE",
    "DESCRIPTION",
    "BASE_ENCOUNTER_COST",
    "TOTAL_CLAIM_COST",
    "PAYER_COVERAGE",
    "REASONCODE",
    "REASONDESCRIPTION",
]

missing_encounter_columns = sorted(
    set(required_encounter_columns)
    - set(encounters_bronze_df.columns)
)

if missing_encounter_columns:
    raise ValueError(
        f"Missing encounter columns: {missing_encounter_columns}"
    )

silver_encounters_df = (
    encounters_bronze_df
    .select(
        F.col("Id").alias("encounter_id"),
        F.to_timestamp("START").alias("start_time"),
        F.to_timestamp("STOP").alias("stop_time"),
        F.col("PATIENT").alias("patient_id"),
        F.col("ORGANIZATION").alias("organization_id"),
        F.col("PROVIDER").alias("provider_id"),
        F.col("PAYER").alias("payer_id"),
        F.lower(
            F.trim("ENCOUNTERCLASS")
        ).alias("encounter_class"),
        F.trim("CODE").alias("encounter_code"),
        F.trim("DESCRIPTION").alias("encounter_description"),
        F.col("BASE_ENCOUNTER_COST")
            .cast("double")
            .alias("base_encounter_cost"),
        F.col("TOTAL_CLAIM_COST")
            .cast("double")
            .alias("total_claim_cost"),
        F.col("PAYER_COVERAGE")
            .cast("double")
            .alias("payer_coverage"),
        F.trim("REASONCODE").alias("reason_code"),
        F.trim("REASONDESCRIPTION").alias("reason_description"),
    )
    .dropDuplicates(["encounter_id"])
    .withColumn(
        "encounter_duration_minutes",
        F.round(
            (
                F.col("stop_time").cast("long")
                - F.col("start_time").cast("long")
            ) / 60.0,
            2,
        ),
    )
    .withColumn(
        "_silver_updated_at",
        F.current_timestamp(),
    )
)

(
    silver_encounters_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.silver_encounters")
)

display(silver_encounters_df.limit(10))

# COMMAND ----------

silver_patients_check_df = spark.table(
    "workspace.default.silver_patients"
)

silver_encounters_check_df = spark.table(
    "workspace.default.silver_encounters"
)

silver_quality_results = [
    (
        "silver_patients",
        silver_patients_check_df.count(),
        len(silver_patients_check_df.columns),
        silver_patients_check_df
            .filter(F.col("patient_id").isNull())
            .count(),
    ),
    (
        "silver_encounters",
        silver_encounters_check_df.count(),
        len(silver_encounters_check_df.columns),
        silver_encounters_check_df
            .filter(F.col("encounter_id").isNull())
            .count(),
    ),
]

silver_quality_df = spark.createDataFrame(
    silver_quality_results,
    [
        "table_name",
        "row_count",
        "column_count",
        "missing_primary_ids",
    ],
)

display(silver_quality_df)

unparsed_encounter_dates = (
    silver_encounters_check_df
    .filter(
        F.col("start_time").isNull()
        | F.col("stop_time").isNull()
    )
    .count()
)

negative_duration_count = (
    silver_encounters_check_df
    .filter(
        F.col("encounter_duration_minutes") < 0
    )
    .count()
)

print(
    f"Encounter rows with unparsed timestamps: "
    f"{unparsed_encounter_dates}"
)

print(
    f"Encounter rows with negative duration: "
    f"{negative_duration_count}"
)

# COMMAND ----------

assert unparsed_encounter_dates == 0, (
    f"Found {unparsed_encounter_dates} encounters with unparsed timestamps."
)

assert negative_duration_count == 0, (
    f"Found {negative_duration_count} encounters with negative durations."
)

print("CORE SILVER QUALITY GATE: PASS")
print(f"Unparsed encounter timestamps: {unparsed_encounter_dates}")
print(f"Negative encounter durations: {negative_duration_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remaining Silver Tables
# MAGIC
# MAGIC This section cleans and standardizes the conditions, medications, procedures, observations, organizations, and providers datasets. Direct personal identifiers that are unnecessary for utilization analysis are excluded.

# COMMAND ----------

from pyspark.sql import functions as F


def require_columns(df, table_name, required_columns):
    """Raise a clear error if expected source columns are missing."""
    missing_columns = sorted(
        set(required_columns) - set(df.columns)
    )

    if missing_columns:
        raise ValueError(
            f"{table_name} is missing required columns: "
            f"{missing_columns}"
        )


def save_silver_table(df, table_name):
    """Write a cleaned dataframe as a managed Silver Delta table."""
    full_table_name = f"workspace.default.silver_{table_name}"

    (
        df.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )

    return (
        full_table_name,
        df.count(),
        len(df.columns),
    )


silver_results = []


# ---------------------------------------------------------
# CONDITIONS
# ---------------------------------------------------------

conditions_bronze_df = spark.table(
    "workspace.default.bronze_conditions"
)

require_columns(
    conditions_bronze_df,
    "bronze_conditions",
    [
        "START",
        "STOP",
        "PATIENT",
        "ENCOUNTER",
        "CODE",
        "DESCRIPTION",
    ],
)

silver_conditions_df = (
    conditions_bronze_df
    .select(
        F.to_date("START").alias("start_date"),
        F.to_date("STOP").alias("stop_date"),
        F.col("PATIENT").alias("patient_id"),
        F.col("ENCOUNTER").alias("encounter_id"),
        F.trim("CODE").alias("condition_code"),
        F.trim("DESCRIPTION").alias("condition_description"),
    )
    .dropDuplicates()
    .withColumn(
        "is_active",
        F.col("stop_date").isNull(),
    )
    .withColumn(
        "_silver_updated_at",
        F.current_timestamp(),
    )
)

silver_results.append(
    save_silver_table(
        silver_conditions_df,
        "conditions",
    )
)


# ---------------------------------------------------------
# MEDICATIONS
# ---------------------------------------------------------

medications_bronze_df = spark.table(
    "workspace.default.bronze_medications"
)

require_columns(
    medications_bronze_df,
    "bronze_medications",
    [
        "START",
        "STOP",
        "PATIENT",
        "PAYER",
        "ENCOUNTER",
        "CODE",
        "DESCRIPTION",
        "BASE_COST",
        "PAYER_COVERAGE",
        "DISPENSES",
        "TOTALCOST",
        "REASONCODE",
        "REASONDESCRIPTION",
    ],
)

silver_medications_df = (
    medications_bronze_df
    .select(
        F.to_timestamp("START").alias("start_time"),
        F.to_timestamp("STOP").alias("stop_time"),
        F.col("PATIENT").alias("patient_id"),
        F.col("ENCOUNTER").alias("encounter_id"),
        F.col("PAYER").alias("payer_id"),
        F.trim("CODE").alias("medication_code"),
        F.trim("DESCRIPTION").alias("medication_description"),
        F.col("BASE_COST").cast("double").alias("base_cost"),
        F.col("PAYER_COVERAGE")
            .cast("double")
            .alias("payer_coverage"),
        F.col("DISPENSES").cast("integer").alias("dispenses"),
        F.col("TOTALCOST").cast("double").alias("total_cost"),
        F.trim("REASONCODE").alias("reason_code"),
        F.trim("REASONDESCRIPTION").alias("reason_description"),
    )
    .dropDuplicates()
    .withColumn(
        "is_active",
        F.col("stop_time").isNull(),
    )
    .withColumn(
        "_silver_updated_at",
        F.current_timestamp(),
    )
)

silver_results.append(
    save_silver_table(
        silver_medications_df,
        "medications",
    )
)


# ---------------------------------------------------------
# PROCEDURES
# ---------------------------------------------------------

procedures_bronze_df = spark.table(
    "workspace.default.bronze_procedures"
)

require_columns(
    procedures_bronze_df,
    "bronze_procedures",
    [
        "START",
        "STOP",
        "PATIENT",
        "ENCOUNTER",
        "CODE",
        "DESCRIPTION",
        "BASE_COST",
        "REASONCODE",
        "REASONDESCRIPTION",
    ],
)

silver_procedures_df = (
    procedures_bronze_df
    .select(
        F.to_timestamp("START").alias("start_time"),
        F.to_timestamp("STOP").alias("stop_time"),
        F.col("PATIENT").alias("patient_id"),
        F.col("ENCOUNTER").alias("encounter_id"),
        F.trim("CODE").alias("procedure_code"),
        F.trim("DESCRIPTION").alias("procedure_description"),
        F.col("BASE_COST").cast("double").alias("base_cost"),
        F.trim("REASONCODE").alias("reason_code"),
        F.trim("REASONDESCRIPTION").alias("reason_description"),
    )
    .dropDuplicates()
    .withColumn(
        "_silver_updated_at",
        F.current_timestamp(),
    )
)

silver_results.append(
    save_silver_table(
        silver_procedures_df,
        "procedures",
    )
)


# ---------------------------------------------------------
# OBSERVATIONS
# ---------------------------------------------------------

observations_bronze_df = spark.table(
    "workspace.default.bronze_observations"
)

require_columns(
    observations_bronze_df,
    "bronze_observations",
    [
        "DATE",
        "PATIENT",
        "ENCOUNTER",
        "CATEGORY",
        "CODE",
        "DESCRIPTION",
        "VALUE",
        "UNITS",
        "TYPE",
    ],
)

silver_observations_df = (
    observations_bronze_df
    .select(
        F.to_timestamp("DATE").alias("observation_time"),
        F.col("PATIENT").alias("patient_id"),
        F.col("ENCOUNTER").alias("encounter_id"),
        F.lower(F.trim("CATEGORY")).alias("category"),
        F.trim("CODE").alias("observation_code"),
        F.trim("DESCRIPTION").alias("observation_description"),
        F.trim("VALUE").alias("observation_value"),
        F.trim("UNITS").alias("units"),
        F.lower(F.trim("TYPE")).alias("value_type"),
    )
    .dropDuplicates()
    .withColumn(
        "_silver_updated_at",
        F.current_timestamp(),
    )
)

silver_results.append(
    save_silver_table(
        silver_observations_df,
        "observations",
    )
)


# ---------------------------------------------------------
# ORGANIZATIONS
# ---------------------------------------------------------

organizations_bronze_df = spark.table(
    "workspace.default.bronze_organizations"
)

require_columns(
    organizations_bronze_df,
    "bronze_organizations",
    [
        "Id",
        "NAME",
        "STATE",
        "ZIP",
        "REVENUE",
        "UTILIZATION",
    ],
)

silver_organizations_df = (
    organizations_bronze_df
    .select(
        F.col("Id").alias("organization_id"),
        F.trim("NAME").alias("organization_name"),
        F.upper(F.trim("STATE")).alias("state"),
        F.trim("ZIP").alias("zip_code"),
        F.col("REVENUE").cast("double").alias("revenue"),
        F.col("UTILIZATION").cast("integer").alias("utilization"),
    )
    .dropDuplicates(["organization_id"])
    .withColumn(
        "_silver_updated_at",
        F.current_timestamp(),
    )
)

silver_results.append(
    save_silver_table(
        silver_organizations_df,
        "organizations",
    )
)


# ---------------------------------------------------------
# PROVIDERS
# ---------------------------------------------------------

providers_bronze_df = spark.table(
    "workspace.default.bronze_providers"
)

require_columns(
    providers_bronze_df,
    "bronze_providers",
    [
        "Id",
        "ORGANIZATION",
        "GENDER",
        "SPECIALITY",
        "ENCOUNTERS",
        "PROCEDURES",
    ],
)

silver_providers_df = (
    providers_bronze_df
    .select(
        F.col("Id").alias("provider_id"),
        F.col("ORGANIZATION").alias("organization_id"),
        F.upper(F.trim("GENDER")).alias("gender"),
        F.trim("SPECIALITY").alias("specialty"),
        F.col("ENCOUNTERS").cast("integer").alias("encounter_count"),
        F.col("PROCEDURES").cast("integer").alias("procedure_count"),
    )
    .dropDuplicates(["provider_id"])
    .withColumn(
        "_silver_updated_at",
        F.current_timestamp(),
    )
)

silver_results.append(
    save_silver_table(
        silver_providers_df,
        "providers",
    )
)


# Display the result of all six writes.
remaining_silver_summary_df = spark.createDataFrame(
    silver_results,
    ["table_name", "row_count", "column_count"],
)

display(
    remaining_silver_summary_df.orderBy("table_name")
)

# COMMAND ----------

silver_table_names = [
    "patients",
    "encounters",
    "conditions",
    "medications",
    "procedures",
    "observations",
    "organizations",
    "providers",
]

silver_validation_results = []

for table_name in silver_table_names:
    full_table_name = f"workspace.default.silver_{table_name}"

    if not spark.catalog.tableExists(full_table_name):
        raise RuntimeError(
            f"Missing Silver table: {full_table_name}"
        )

    current_df = spark.table(full_table_name)

    silver_validation_results.append(
        (
            full_table_name,
            current_df.count(),
            len(current_df.columns),
        )
    )

all_silver_tables_df = spark.createDataFrame(
    silver_validation_results,
    ["table_name", "row_count", "column_count"],
)

display(
    all_silver_tables_df.orderBy("table_name")
)

print("ALL EIGHT SILVER TABLES CREATED")

# COMMAND ----------
