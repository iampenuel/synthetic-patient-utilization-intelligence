# Findings And Limitations

## Findings

1. Utilization was concentrated among a smaller number of synthetic patients.
2. The 65+ age group recorded the highest average encounter volume.
3. Ambulatory encounters represented the largest encounter category.
4. Recorded claim costs generally increased with encounter volume, but the relationship was not uniform.
5. High-utilization records included disorders, social findings, administrative situations, and environmental observations.
6. Healthcare analytics systems must distinguish diagnoses from observations, situations, and social-context records.

## Analytical Counts

| Metric | Value |
| --- | ---: |
| Synthetic patients | 113 |
| Encounters | 7,210 |
| Average encounters per patient | 63.81 |
| Emergency encounters | 238 |
| Synthetic recorded claim costs | ~$16.3M |
| Condition records | 4,748 |
| Medication records | 8,180 |
| Procedure records | 20,527 |
| Observation records | 114,335 |

## Limitations

- The data is synthetic and does not represent a real healthcare population.
- Results should not be generalized to a real hospital, insurer, health system, or geographic population.
- Recorded claim costs are synthetic and are not validated billing, reimbursement, or accounting figures.
- A repeat encounter within 30 days is an operational timing feature, not a validated hospital-readmission measure.
- The high-utilization threshold is descriptive and should not be interpreted as a clinical-risk classification.
- Condition records include disorders, findings, situations, and social-context concepts; they are not all diagnoses.
- The analysis identifies associations, not causal relationships.
- The dataset contains only 113 synthetic patients and is suitable for portfolio-scale demonstration rather than production benchmarking.
- Version 1 does not contain a trained machine-learning model.
