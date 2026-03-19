# dbt Observability Project

Real-time observability for dbt Cloud jobs and data quality metrics on Databricks.

## Overview

This dbt project transforms dbt Cloud metrics into actionable observability data:
- Job performance tracking
- Data quality monitoring  
- SLA compliance
- Test failure detection

## Data Models

### Bronze Layer (staging/)
- `stg_dbt_runs` - Raw job run data
- `stg_dbt_test_results` - Raw test result data

### Silver Layer (intermediate/)
- `int_run_metrics` - Cleaned run metrics
- `int_test_metrics` - Cleaned test metrics

### Gold Layer (marts/)
- `dim_jobs` - Job dimensions
- `fct_job_runs` - Job run facts
- `fct_test_results` - Test result facts
- `fct_sla_tracking` - SLA violations
