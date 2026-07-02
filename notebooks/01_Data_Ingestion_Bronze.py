# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Data Ingestion
# MAGIC
# MAGIC This notebook inventories and validates the original Synthea CSV files stored in the `synthea_raw` Unity Catalog volume. The raw source files are preserved without transformation before being loaded into Bronze Delta tables.

# COMMAND ----------

volume_path = (
    "/Volumes/workspace/default/"
    "synthea_raw/synthea_sample_data_csv_latest"
)

uploaded_files = dbutils.fs.ls(volume_path)

file_inventory = [
    (
        file.name,
        file.size,
        file.path
    )
    for file in uploaded_files
]

files_df = spark.createDataFrame(
    file_inventory,
    ["file_name", "size_bytes", "file_path"]
)

display(files_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Core Source Data Audit
# MAGIC
# MAGIC The first pipeline version focuses on eight Synthea tables that describe patients, healthcare encounters, clinical activity, and the organizations and providers involved in care.
# MAGIC
# MAGIC During this audit, all CSV columns are initially read as strings so that the raw source values remain unchanged. Data types and transformations will be applied later in the Silver layer.

# COMMAND ----------

from pyspark.sql import Row

base_path = (
    "/Volumes/workspace/default/"
    "synthea_raw/synthea_sample_data_csv_latest"
)

core_files = [
    "patients.csv",
    "encounters.csv",
    "conditions.csv",
    "medications.csv",
    "procedures.csv",
    "observations.csv",
    "organizations.csv",
    "providers.csv",
]

# Confirm that every required file exists before continuing.
available_files = {
    file.name
    for file in dbutils.fs.ls(base_path)
}

missing_files = [
    file_name
    for file_name in core_files
    if file_name not in available_files
]

if missing_files:
    raise FileNotFoundError(
        f"Missing required files: {missing_files}"
    )

audit_results = []

for file_name in core_files:
    file_path = f"{base_path}/{file_name}"

    raw_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "FAILFAST")
        .csv(file_path)
    )

    audit_results.append(
        Row(
            file_name=file_name,
            row_count=raw_df.count(),
            column_count=len(raw_df.columns),
        )
    )

audit_df = (
    spark.createDataFrame(audit_results)
    .orderBy("file_name")
)

display(audit_df)

# COMMAND ----------

patients_path = f"{base_path}/patients.csv"

patients_raw_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .option("mode", "FAILFAST")
    .csv(patients_path)
)

print("Patients table columns:")
print(patients_raw_df.columns)

print("\nPatients table schema:")
patients_raw_df.printSchema()

display(patients_raw_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Delta Table Creation
# MAGIC
# MAGIC The following step loads the eight core Synthea CSV files without changing their source values, adds ingestion metadata, and stores each dataset as a managed Bronze Delta table.

# COMMAND ----------

from pyspark.sql import functions as F

base_path = (
    "/Volumes/workspace/default/"
    "synthea_raw/synthea_sample_data_csv_latest"
)

core_tables = [
    "patients",
    "encounters",
    "conditions",
    "medications",
    "procedures",
    "observations",
    "organizations",
    "providers",
]

bronze_results = []

for table_name in core_tables:
    file_path = f"{base_path}/{table_name}.csv"
    output_table = f"workspace.default.bronze_{table_name}"

    # Keep all original CSV values as strings in the Bronze layer.
    raw_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "FAILFAST")
        .csv(file_path)
    )

    # Add traceability metadata without changing source columns.
    bronze_df = (
        raw_df
        .withColumn("_source_file", F.lit(f"{table_name}.csv"))
        .withColumn("_source_path", F.lit(file_path))
        .withColumn("_ingested_at", F.current_timestamp())
    )

    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(output_table)
    )

    bronze_results.append(
        (
            output_table,
            bronze_df.count(),
            len(bronze_df.columns),
        )
    )

bronze_summary_df = spark.createDataFrame(
    bronze_results,
    ["table_name", "row_count", "column_count"],
)

display(bronze_summary_df.orderBy("table_name"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN workspace.default LIKE 'bronze_*';

# COMMAND ----------

patients_bronze_df = spark.table(
    "workspace.default.bronze_patients"
)

patient_id_check = patients_bronze_df.agg(
    F.count("*").alias("total_rows"),
    F.count("Id").alias("non_null_ids"),
    F.countDistinct("Id").alias("distinct_ids"),
)

display(patient_id_check)

# COMMAND ----------

encounters_bronze_df = spark.table(
    "workspace.default.bronze_encounters"
)

patient_ids_df = patients_bronze_df.select(
    F.col("Id").alias("patient_id")
)

orphan_encounters_df = (
    encounters_bronze_df.alias("encounter")
    .join(
        patient_ids_df.alias("patient"),
        F.col("encounter.PATIENT") == F.col("patient.patient_id"),
        "left_anti",
    )
)

orphan_count = orphan_encounters_df.count()

print(f"Encounter records without a matching patient: {orphan_count}")

display(
    orphan_encounters_df
    .select("Id", "PATIENT")
    .limit(20)
)

# COMMAND ----------

from pyspark.sql import functions as F

core_tables = [
    "patients",
    "encounters",
    "conditions",
    "medications",
    "procedures",
    "observations",
    "organizations",
    "providers",
]

table_results = []

for table_name in core_tables:
    full_table_name = f"workspace.default.bronze_{table_name}"

    if not spark.catalog.tableExists(full_table_name):
        raise RuntimeError(
            f"Required Bronze table does not exist: {full_table_name}"
        )

    table_df = spark.table(full_table_name)

    table_results.append(
        (
            full_table_name,
            table_df.count(),
            len(table_df.columns),
        )
    )

bronze_validation_df = spark.createDataFrame(
    table_results,
    ["table_name", "row_count", "column_count"],
)

display(bronze_validation_df.orderBy("table_name"))

patients_df = spark.table("workspace.default.bronze_patients")
encounters_df = spark.table("workspace.default.bronze_encounters")

patient_stats = (
    patients_df
    .agg(
        F.count("*").alias("total_rows"),
        F.count("Id").alias("non_null_ids"),
        F.countDistinct("Id").alias("distinct_ids"),
    )
    .first()
    .asDict()
)

encounter_stats = (
    encounters_df
    .agg(
        F.count("*").alias("total_rows"),
        F.count("Id").alias("non_null_ids"),
        F.countDistinct("Id").alias("distinct_ids"),
    )
    .first()
    .asDict()
)

orphan_encounter_count = (
    encounters_df.alias("encounter")
    .join(
        patients_df.select(
            F.col("Id").alias("patient_id")
        ).alias("patient"),
        F.col("encounter.PATIENT") == F.col("patient.patient_id"),
        "left_anti",
    )
    .count()
)

assert patient_stats["total_rows"] == patient_stats["non_null_ids"], (
    "Some patient IDs are missing."
)

assert patient_stats["total_rows"] == patient_stats["distinct_ids"], (
    "Duplicate patient IDs were found."
)

assert encounter_stats["total_rows"] == encounter_stats["non_null_ids"], (
    "Some encounter IDs are missing."
)

assert encounter_stats["total_rows"] == encounter_stats["distinct_ids"], (
    "Duplicate encounter IDs were found."
)

assert orphan_encounter_count == 0, (
    f"Found {orphan_encounter_count} encounters without patients."
)

print("BRONZE QUALITY GATE: PASS")
print(f"Patient ID check: {patient_stats}")
print(f"Encounter ID check: {encounter_stats}")
print(f"Orphan encounters: {orphan_encounter_count}")

# COMMAND ----------
