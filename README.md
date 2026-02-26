# Healthcare Claims Analytics Engineering

dbt • Snowflake • Streamlit

------------------------------------------------------------------------

## Live Interactive Dashboard

Streamlit App:\
https://healthcare-claims-analytics-dbt-ljxtrfq2zwtcdfsopfkxqc.streamlit.app/

This live dashboard is powered directly from Snowflake analytics marts
built using dbt Core. It demonstrates an end-to-end Analytics
Engineering workflow from raw ingestion to dimensional modeling,
incremental processing, SCD Type 2 tracking, and production-ready BI
deployment.

------------------------------------------------------------------------

## Project Overview

This project simulates a healthcare claims data warehouse and analytics
layer using modern Analytics Engineering best practices.

It demonstrates:

-   Source modeling from raw operational tables
-   Structured staging transformations
-   Dimensional modeling (star schema design)
-   Incremental fact processing with late-arriving data handling
-   SCD Type 2 historical tracking
-   Data quality testing with dbt
-   Secure Snowflake key-pair authentication
-   Deployed analytics dashboard using Streamlit Cloud

Pipeline Flow:

RAW → STAGING → DIMENSIONS → INCREMENTAL FACTS\
→ ANALYTICS MARTS → SNAPSHOTS → DASHBOARD

------------------------------------------------------------------------

## Technology Stack

-   dbt Core
-   Snowflake
-   Streamlit (Cloud Deployment)
-   Plotly
-   GitHub

------------------------------------------------------------------------

## Data Architecture

### Source Layer (RAW Schema)

-   raw_claims_header\
-   raw_claims_line\
-   raw_ip_claims\
-   raw_members\
-   raw_providers

These tables simulate operational healthcare claims systems.

------------------------------------------------------------------------

### Staging Layer (Views)

-   stg_claims_header\
-   stg_claims_line\
-   stg_ip_claims\
-   stg_members\
-   stg_providers

The staging layer standardizes field names, enforces data types, and
applies initial transformations.

------------------------------------------------------------------------

### Dimensional Models (Tables)

-   dim_member\
-   dim_provider\
-   dim_member_current\
-   dim_provider_current

The \*\_current models surface active records derived from SCD Type 2
snapshot history.

------------------------------------------------------------------------

### Fact Tables

#### fct_claim_line

-   Materialization: incremental\
-   Grain: claim_id + line_num\
-   Merge strategy using unique_key\
-   Lookback window to simulate late-arriving claims

#### fct_ip_claim

-   Grain: inpatient stay\
-   DRG-based modeling\
-   Length-of-stay and reimbursement metrics

------------------------------------------------------------------------

### Analytics Marts (Powering the Dashboard)

-   PBI_FINANCIAL_KPIS\
-   PBI_MONTHLY_TRENDS\
-   PBI_UTILIZATION_BY_PAYER\
-   PBI_PROVIDER_SPECIALTY_MIX\
-   PBI_IP_DRG_SUMMARY

These marts are optimized for BI consumption and drive the deployed
Streamlit dashboard.

------------------------------------------------------------------------

## Incremental Strategy

The fct_claim_line model is materialized incrementally using a merge
strategy with a defined unique key.

To simulate real-world claims processing:

-   A lookback window reprocesses recent data
-   Late-arriving records are reconciled
-   Schema changes are synchronized using dbt configuration

------------------------------------------------------------------------

## SCD Type 2 Snapshots

Provider and member attribute changes are tracked over time using dbt
snapshots.

snap_dim_provider\
Tracks provider group and specialty changes.

snap_dim_member\
Tracks insurance plan changes.

This enables:

-   Historical point-in-time analysis
-   Accurate "as-of" joins
-   Temporal provider and member analytics

------------------------------------------------------------------------

## Data Quality Controls

Implemented using dbt tests:

-   not_null and unique tests on dimension keys
-   relationship tests enforcing referential integrity
-   accepted_values test on payer_type classification

These safeguards ensure analytical reliability across downstream marts.

------------------------------------------------------------------------

## Dashboard Features

The Streamlit dashboard includes:

-   Executive KPI ribbon (Claims, Members, Allowed, Paid, efficiency
    metrics)
-   Payer reimbursement comparisons
-   Monthly financial trend analysis
-   DRG-level inpatient analysis
-   Provider specialty mix treemap
-   Downloadable CSV exports for each analytical section

The application connects securely to Snowflake using key-pair
authentication. No credentials or private keys are stored in the
repository.

------------------------------------------------------------------------

## Security and Deployment

The dashboard is deployed via Streamlit Cloud.

Authentication is handled through Snowflake key-pair authentication with
secrets stored securely in the deployment environment.

Sensitive files excluded via .gitignore:

-   .streamlit/secrets.toml\
-   \*.p8\
-   \*.pem

------------------------------------------------------------------------

## Running Locally

### 1. Create Environment

python -m venv .venv\
.venv`\Scripts`{=tex}`\activate  `{=tex} pip install -r requirements.txt

### 2. Run dbt

dbt run\
dbt test\
dbt snapshot

### 3. Launch Streamlit

Local development uses a file-based private key path defined in
secrets.toml:

snowflake_private_key_path = "C:\\path\\to\\rsa_key.p8"

Then run:

streamlit run claims_ae/streamlit/streamlit_app.py

------------------------------------------------------------------------

## What This Project Demonstrates

-   End-to-end Analytics Engineering lifecycle
-   Dimensional modeling best practices
-   Incremental data pipeline design
-   Late-arriving data handling
-   SCD Type 2 implementation
-   Data quality enforcement
-   Secure cloud BI deployment
-   Production-ready portfolio architecture
