# Data

This project uses Synthea-generated synthetic healthcare data. Raw data is intentionally not committed to this public repository.

## Source

Use the official Synthea project to generate or obtain synthetic patient CSV data:

- Synthea GitHub repository: https://github.com/synthetichealth/synthea

External datasets and platforms remain governed by their own terms and licenses.

## Expected Files

The core pipeline expects these CSV files in a Databricks Unity Catalog volume:

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

The supplied project context confirms 18 uploaded source CSV files, while the first pipeline version processes the eight core domains above.

## Expected Volume Path Pattern

The exported notebooks use this default path:

```text
/Volumes/workspace/default/synthea_raw/synthea_sample_data_csv_latest
```

If your catalog, schema, volume, or folder names differ, update the notebook path variables consistently before running the pipeline.

## Privacy Boundary

Do not place real patient data in this public repository. Do not commit raw Synthea CSV files, private Databricks exports, access credentials, environment files, or files containing authentication information.
