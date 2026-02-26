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

## Architecture Overview

![Lineage DAG](docs/lineage_dag.png)

This lineage graph illustrates the full transformation pipeline from raw
ingestion through staging, dimensional modeling, incremental facts,
analytics marts, and final dashboard consumption.

Pipeline Flow:

RAW → STAGING → DIMENSIONS → INCREMENTAL FACTS\
→ ANALYTICS MARTS → SNAPSHOTS → DASHBOARD

------------------------------------------------------------------------

## Data Architecture

### Source Layer (RAW Schema)

-   raw_claims_header\
-   raw_claims_line\
-   raw_ip_claims\
-   raw_members\
-   raw_providers

Example source definition documentation:

![Source Documentation](docs/source_raw_claims_line.png)

------------------------------------------------------------------------

### Staging Layer (Views)

-   stg_claims_header\
-   stg_claims_line\
-   stg_ip_claims\
-   stg_members\
-   stg_providers

The staging layer standardizes naming, enforces data types, and prepares
models for dimensional transformation.

------------------------------------------------------------------------

### Dimensional Models

-   dim_member\
-   dim_provider\
-   dim_member_current\
-   dim_provider_current

The \*\_current models surface active records derived from SCD Type 2
snapshots.

------------------------------------------------------------------------

### Fact Tables

#### fct_claim_line

-   Materialization: incremental\
-   Grain: claim_id + line_num\
-   Merge strategy using unique_key\
-   Lookback window to simulate late-arriving claims

Model documentation example:

![Fact Model Documentation](docs/fct_claim_line_tests.png)

#### fct_ip_claim

-   Grain: inpatient stay\
-   DRG-based modeling\
-   Length-of-stay and reimbursement metrics

------------------------------------------------------------------------

## SCD Type 2 Snapshots

Provider and member attributes are tracked historically using dbt
snapshots.

snap_dim_provider tracks provider group and specialty changes.\
snap_dim_member tracks insurance plan changes.

Snapshot history example:

![Snapshot History](docs/snapshot_provider_history.png)

This enables:

-   Historical point-in-time analysis\
-   Accurate as-of joins\
-   Temporal provider and member analytics

------------------------------------------------------------------------

## Analytics Marts (Powering the Dashboard)

-   PBI_FINANCIAL_KPIS\
-   PBI_MONTHLY_TRENDS\
-   PBI_UTILIZATION_BY_PAYER\
-   PBI_PROVIDER_SPECIALTY_MIX\
-   PBI_IP_DRG_SUMMARY

These marts are optimized for BI consumption and power the deployed
Streamlit dashboard.

------------------------------------------------------------------------

## Data Quality Controls

Implemented using dbt tests:

-   not_null and unique tests on dimension keys\
-   relationship tests enforcing referential integrity\
-   accepted_values tests on payer_type classification

These safeguards ensure analytical reliability across downstream models.

------------------------------------------------------------------------

## Security and Deployment

The dashboard is deployed via Streamlit Cloud using Snowflake key-pair
authentication.

Sensitive files excluded via .gitignore:

-   .streamlit/secrets.toml\
-   \*.p8\
-   \*.pem

Private keys are stored securely in the Streamlit deployment environment
and are not committed to the repository.

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

streamlit run claims_ae/streamlit/streamlit_app.py

------------------------------------------------------------------------

## What This Project Demonstrates

-   End-to-end Analytics Engineering lifecycle\
-   Dimensional modeling best practices\
-   Incremental data pipeline design\
-   Late-arriving data handling\
-   SCD Type 2 implementation\
-   Data quality enforcement\
-   Secure cloud BI deployment\
-   Production-ready portfolio architecture
