# Agent Guidance

## Project Purpose

Synthetic Patient Utilization Intelligence is a Databricks healthcare analytics portfolio project. It transforms Synthea-generated synthetic CSV records through Bronze, Silver, and Gold layers into patient-level utilization analytics and an AI/BI dashboard.

## Verified Metrics

- 18 source CSV files uploaded to a Unity Catalog volume
- 8 core source domains processed
- 8 Bronze Delta tables
- 8 Silver Delta tables
- 1 Gold patient-utilization table
- 113 synthetic patients
- 7,210 encounters
- 63.81 average encounters per patient
- 238 emergency encounters
- Approximately $16.3M in synthetic recorded claim costs
- 4,748 condition records
- 8,180 medication records
- 20,527 procedure records
- 114,335 observation records

## Prohibited Claims

Do not claim that this repository:

- uses real patient data;
- contains a trained machine-learning model;
- provides clinical decision support;
- diagnoses disease;
- recommends treatment;
- determines medical necessity;
- predicts individual clinical outcomes;
- prioritizes real patients;
- validates readmission predictions;
- improves patient outcomes;
- reduces healthcare costs;
- contains real billing, reimbursement, revenue, savings, or business impact.

## Validation Commands

Run these before committing:

```bash
python scripts/validate_repository.py
python -m compileall notebooks
git diff --check
find . -type f -size +20M
git status --short
```

## Privacy Requirements

- No real protected health information belongs in this repository.
- Do not commit raw Synthea CSV files.
- Do not commit ZIP exports, `.env` files, credentials, tokens, cookies, private workspace URLs, or authentication artifacts.
- Exclude screenshots that expose email addresses, account identifiers, authentication controls, browser chrome, private workspace URLs, or unrelated personal content.
- Treat synthetic identifier-like fields with privacy-aware caution.

## Technical Honesty

Keep claims tied to notebook evidence and supplied screenshots. Preserve Databricks notebook source markers and functioning notebook logic unless a focused correction is necessary. Future work can mention MLflow-based utilization modeling, but Version 1 must remain framed as data engineering, SQL analytics, and dashboarding.
