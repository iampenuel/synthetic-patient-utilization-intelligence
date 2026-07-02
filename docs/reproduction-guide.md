# Reproduction Guide

This guide describes how to reproduce the Databricks workflow with Synthea-generated synthetic CSV data.

## Prerequisites

- Databricks workspace with Unity Catalog support
- Apache Spark runtime through Databricks
- Permission to create or use a Unity Catalog volume
- Synthea CSV source files

## Source Data

Generate or obtain Synthea CSV files from the official Synthea project. The notebooks expect files under this pattern by default:

```text
/Volumes/workspace/default/synthea_raw/synthea_sample_data_csv_latest
```

The first pipeline version requires these eight files:

```text
patients.csv
encounters.csv
conditions.csv
medications.csv
procedures.csv
observations.csv
organizations.csv
providers.csv
```

The project context confirms 18 total uploaded CSV files, but the core pipeline processes the eight files listed above.

## Execution Order

Run the notebooks in numeric order:

1. `00_Project_Overview.py`
2. `01_Data_Ingestion_Bronze.py`
3. `02_Data_Cleaning_Silver.py`
4. `03_Patient_Utilization_Gold.py`
5. `04_Utilization_Analytics.py`
6. `05_Findings_and_Limitations.py`

## Expected Outputs

- 8 Bronze Delta tables
- 8 Silver Delta tables
- 1 Gold patient-utilization table
- SQL analytics outputs
- Databricks AI/BI dashboard built from the Gold and Silver analytical outputs

## Local Repository Checks

The local repository validation script checks portfolio hygiene, not Spark computation correctness:

```bash
python scripts/validate_repository.py
python -m compileall notebooks
git diff --check
find . -type f -size +20M
git status --short
```

## Notes

Do not commit raw Synthea CSV files, private Databricks exports, credentials, environment files, or screenshots that expose private workspace details.
