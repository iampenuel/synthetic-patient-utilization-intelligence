# Quality And Validation

The validation described here is based on checks represented in the exported Databricks notebooks. It should be read as repository and pipeline evidence, not as a production data-governance certification.

## Checks Represented In The Notebooks

| Check | Location | Result or purpose |
| --- | --- | --- |
| Uploaded file inventory | `01_Data_Ingestion_Bronze.py` | Lists files in the Unity Catalog volume. |
| Required source files | `01_Data_Ingestion_Bronze.py` | Raises an error if any of the eight core CSV files are missing. |
| Row and column counts | Bronze and Silver notebooks | Reports table-level counts for traceability. |
| Missing patient IDs | `01_Data_Ingestion_Bronze.py`, `02_Data_Cleaning_Silver.py` | Checks non-null patient identifiers. |
| Duplicate patient IDs | `01_Data_Ingestion_Bronze.py` | Checks distinct patient IDs against row counts. |
| Missing encounter IDs | `01_Data_Ingestion_Bronze.py`, `02_Data_Cleaning_Silver.py` | Checks non-null encounter identifiers. |
| Duplicate encounter IDs | `01_Data_Ingestion_Bronze.py` | Checks distinct encounter IDs against row counts. |
| Encounter-to-patient relationships | `01_Data_Ingestion_Bronze.py` | Left-anti join identifies encounters without matching patients. |
| Timestamp parsing | `02_Data_Cleaning_Silver.py` | Counts encounters with unparsed start/stop timestamps. |
| Negative encounter duration | `02_Data_Cleaning_Silver.py` | Asserts no negative encounter duration rows. |
| Gold one-row-per-patient validation | `03_Patient_Utilization_Gold.py` | Asserts 113 Gold rows and 113 non-null patient IDs. |
| Gold-to-Silver encounter reconciliation | `03_Patient_Utilization_Gold.py` | Asserts Gold total encounters equals 7,210 Silver encounters. |

## Confirmed Validation Results

- Patient identifiers were checked for nulls and duplicates.
- Encounter identifiers were checked for nulls and duplicates.
- Encounter-to-patient relationships were validated.
- The visible orphan-encounter result was 0.
- Gold was validated as one row per synthetic patient.
- Gold encounter totals reconcile with 7,210 Silver encounters.

## Descriptive Metrics

- 113 synthetic patients
- 7,210 encounters
- 63.81 average encounters per patient
- 238 emergency encounters
- Approximately $16.3M in synthetic recorded claim costs
- 4,748 condition records
- 8,180 medication records
- 20,527 procedure records
- 114,335 observation records

The $16.3M figure is synthetic recorded claim cost only. It is not real revenue, reimbursement, billing, savings, or business impact.

## Assumptions

- The eight core Synthea CSV files are present in the configured Unity Catalog volume path.
- The exported notebooks are run in numeric order.
- Databricks catalog and schema names match the notebook defaults or are adjusted consistently before execution.

## Future Production-Grade Validation Work

- Add formal data-quality expectations with thresholds.
- Add pipeline monitoring and alerting.
- Add source schema drift checks.
- Add referential integrity checks across all relationship-bearing tables.
- Add versioned data contracts.
- Add dashboard refresh monitoring.
- Add privacy and access-control review before any real-world adaptation.
