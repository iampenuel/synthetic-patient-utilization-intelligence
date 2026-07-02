# Data Model

This document describes the inspected table-level model. It intentionally avoids a fake field-by-field dictionary for columns that were not fully documented in the exports.

## Bronze Tables

| Table | Description |
| --- | --- |
| `bronze_patients` | Raw Synthea patient records, preserved as source strings with ingestion metadata. |
| `bronze_encounters` | Raw encounter records, including patient, organization, provider, encounter class, timestamps, and cost fields. |
| `bronze_conditions` | Raw condition/finding/situation records associated with patients and encounters. |
| `bronze_medications` | Raw medication records associated with patients, encounters, and payer/cost context. |
| `bronze_procedures` | Raw procedure records associated with patients and encounters. |
| `bronze_observations` | Raw observation records, including dated coded observations and values. |
| `bronze_organizations` | Raw organization reference records. |
| `bronze_providers` | Raw provider reference records. |

## Silver Tables

| Table | Description |
| --- | --- |
| `silver_patients` | Cleaned patient attributes with typed dates, demographics, geography, healthcare expense/coverage fields, income, and deceased status. |
| `silver_encounters` | Cleaned encounter records with typed timestamps, standardized encounter class, typed cost fields, duration minutes, and patient/organization/provider relationships. |
| `silver_conditions` | Cleaned condition records with standardized identifiers, dates, codes, and descriptions. |
| `silver_medications` | Cleaned medication records with typed dates, standardized codes/descriptions, and typed cost fields. |
| `silver_procedures` | Cleaned procedure records with typed timestamps, codes, descriptions, and encounter relationships. |
| `silver_observations` | Cleaned observation records with typed observation time, codes, descriptions, values, units, and patient/encounter relationships. |
| `silver_organizations` | Cleaned organization records with organization identifiers and reference attributes. |
| `silver_providers` | Cleaned provider records with provider identifiers and organization relationships. |

## Gold Table

`gold_patient_utilization` is the primary analytical table. Each row represents one synthetic patient.

Key feature groups include:

- patient demographics and age group;
- total, emergency, inpatient, outpatient, ambulatory, and urgent-care encounter counts;
- repeat encounters within 30 days as a descriptive timing feature;
- total synthetic recorded claim cost;
- distinct condition count;
- distinct medication count;
- procedure record count;
- organization and provider touchpoints;
- most recent encounter timing.

## Primary Identifiers And Relationship Keys

| Key | Role |
| --- | --- |
| `patient_id` | Main patient relationship key across patient, encounter, condition, medication, procedure, observation, and Gold records. |
| `encounter_id` | Primary encounter identifier and relationship key for encounter-linked clinical activity. |
| `organization_id` | Relationship key connecting encounters and providers to organizations. |
| `provider_id` | Relationship key connecting encounters to providers. |

## Data Minimization

Analytical Silver and Gold outputs exclude unnecessary direct identifier-like fields where they are not needed for utilization analysis. The notebooks specifically document exclusion of names, synthetic SSNs, passport values, driver-license values, and street addresses where unnecessary.
