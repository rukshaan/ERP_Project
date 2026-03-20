# ERP Project - Recent Accomplishments Summary

**Date:** March 20, 2026  
**Project:** Sales Data Platform with Airflow, DBT, Delta Lake & Streamlit

---

## 🎯 Overview

This document summarizes the key accomplishments and improvements made to the ERP data pipeline project, focusing on resolving critical issues in the DBT transformation layer, Delta Lake concurrency handling, and data model dependencies.

---

## 📋 Key Accomplishments

### 1. **DBT Model Dependency Resolution** ✅

#### Problem
- `dim_company` model was failing during compilation due to undefined snapshot reference (`snx_company_snapshot`)
- Similar dependency issues existed across multiple dimension models relying on snapshots that weren't being created

#### Solution
- **Refactored `dim_company.sql`** to source from staging layer (`stg_sales_order`) instead of snapshots
- Eliminated duplicate SQL blocks and legacy snapshot-based logic
- Simplified surrogate key generation using `dbt_utils.generate_surrogate_key()`
- Removed dependency on `dbt snapshot` step for company dimension

#### Impact
- ✅ Resolved compilation failures
- ✅ Simplified data flow architecture
- ✅ Reduced pipeline complexity by eliminating unnecessary snapshot dependencies

---

### 2. **Staging Layer Column Mapping Fixes** ✅

#### Problem
- `stg_customer` model had column name mismatches between Silver layer output and DBT expectations
- Runtime error: `customer_name` column not found (actual column was `customer`)

#### Solution
- Added column aliasing: `customer AS customer_name` in SELECT clause
- Removed all commented `ref()` lines that were causing compilation parsing issues
- Temporarily used `delta_scan('/opt/airflow/data/Silver/delta/Customer')` to bypass snapshot dependency
- Ensured proper incremental processing with `max_loaded` CTE pattern

#### Impact
- ✅ Fixed Binder runtime errors
- ✅ Aligned column names with downstream dimension model expectations
- ✅ Enabled incremental data loading

---

### 3. **Delta Lake Concurrency Error Resolution** ✅

#### Problem
- Spark tasks failing with `DELTA_PROTOCOL_CHANGED` errors during Silver layer transformations
- Race conditions occurring when concurrent jobs wrote to empty Delta directories simultaneously
- Dual-write pattern (empty table creation + data write) creating unsafe concurrent operations

#### Solution
- **Created `delta_utils.py`** with reusable utility functions:
  - `safe_delta_write()` - Retry logic with exponential backoff for concurrency errors
  - `delta_table_exists()` - Safe table existence checking
  - `create_empty_delta_table()` - Schema initialization utility
  
- **Removed unsafe dual-write patterns** from:
  - `transform_salesorder_to_silver.py`
  - `task2.py` (Silver transformation task)
  
- Implemented retry mechanism with:
  - Exponential backoff (wait time = retry_count × 2 seconds)
  - Fallback to `mergeSchema` option on protocol change errors
  - Safe overwrite using `replaceWhere="1=1"` to prevent race conditions

#### Impact
- ✅ Eliminated concurrency-related pipeline failures
- ✅ Improved reliability of parallel Spark job execution
- ✅ Created reusable pattern for future Delta Lake operations

---

### 4. **Data Pipeline Architecture Improvements** ✅

#### Current Architecture Flow
```
Bronze Layer (Raw JSON Ingestion)
    ↓
Silver Layer (Transformation & Standardization)
    ↓
Staging Layer (DBT - Current Records Only)
    ↓
Dimension Layer (DBT - Deduplicated Dimensions)
    ↓
Mart Layer (DBT - Business Metrics)
```

#### Improvements Made
- **Simplified dependency chain**: Moved from snapshot-dependent to staging-dependent models
- **Incremental processing**: Implemented proper incremental patterns in staging models
- **Data quality**: Added COALESCE for null handling (`'N/A'` defaults)
- **Date handling**: Proper date casting for creation/modification timestamps

---

## 🔧 Technical Components Enhanced

### Airflow DAGs
- **`dbt_transformation_dag.py`**: Hourly scheduled DBT transformations
  - Task flow: `debug → deps → snapshot → run → test → docs_generate → docs_serve`
  - Built-in retry logic with exponential backoff
  - Integrated DBT debugging as first-class task

### DBT Models
**Staging Layer:**
- ✅ `stg_customer.sql` - Customer data with incremental loading
- ✅ `stg_item.sql` - Item standardization
- ✅ `stg_sales_order.sql` - Sales order aggregation

**Core/Dimension Layer:**
- ✅ `dim_company.sql` - Company dimension (fixed dependency)
- ✅ `dim_customer.sql` - Customer dimension
- ✅ `dim_item.sql` - Item dimension
- ✅ `dim_date.sql` - Date dimension
- ✅ `fact_final_sales_order_file.sql` - Fact table

**Snapshot Layer:**
- Company, Customer, Item snapshots (SCD-2 Type 2 slowly changing dimensions)

### Delta Lake Utilities
- **Path:** `dags/tasks/delta_utils.py`
- Handles concurrent write conflicts
- Provides safe table initialization
- Implements retry logic with exponential backoff

---

## 📊 Data Flow Summary

### Bronze → Silver Transformations
1. **Customer**: Raw JSON → Standardized schema with type casting
2. **Item**: Product data → Cleaned item master
3. **Sales Order**: Order headers → Aggregated order facts
4. **Quotation**: Quote records → Enriched quotation data
5. **Sales Invoice**: Invoice records → Validated invoice facts

### Silver → DBT Staging
- Direct Delta Lake reads via `delta_scan()`
- Incremental filtering based on max loaded timestamp
- Current record selection (for eventual SCD-2 snapshot integration)

### DBT Staging → Dimensions
- Deduplication with `ROW_NUMBER()` window function
- Surrogate key generation using `dbt_utils`
- Null handling with default values

---

## 🚀 Performance & Reliability Improvements

| Issue | Before | After |
|-------|--------|-------|
| **DBT Compilation Errors** | Frequent failures due to undefined refs | ✅ All models compile successfully |
| **Delta Concurrency** | DELTA_PROTOCOL_CHANGED errors | ✅ Safe writes with retry logic |
| **Pipeline Dependencies** | Complex snapshot dependencies | ✅ Simplified staging-based flow |
| **Column Mismatches** | Runtime binder errors | ✅ Proper column aliasing |
| **Code Reusability** | Duplicate logic across tasks | ✅ Centralized delta_utils.py |

---

## 🛠️ Technology Stack

- **Orchestration**: Apache Airflow
- **Transformation**: DBT Core
- **Storage**: Delta Lake 3.2.0
- **Processing**: Spark 3.5.3
- **Analytics**: DuckDB (development), Streamlit (UI)
- **Frontend**: Streamlit Dashboard

---

## 📈 Current Status

### ✅ Working Components
- Bronze layer ingestion (Customer, Item, Sales Order, Quotation, Sales Invoice)
- Silver layer transformations with Delta Lake storage
- DBT staging models with incremental processing
- DBT dimension models with surrogate keys
- DBT fact tables for analytics
- Delta Lake concurrency handling
- Airflow DAG scheduling (hourly)

### 🔄 Optimized Patterns
- Snapshot-independent dimension models
- Safe Delta write operations
- Incremental data loading
- Error retry mechanisms

---

## 🎓 Lessons Learned

1. **Snapshot Complexity**: While SCD-2 snapshots provide historical tracking, they add significant complexity. Starting with staging-only models simplifies initial development.

2. **Delta Lake Concurrency**: Empty Delta tables created concurrently can cause protocol conflicts. Using `replaceWhere="1=1"` with retry logic prevents race conditions.

3. **Column Naming Consistency**: Early alignment between Silver layer output columns and DBT model expectations prevents runtime errors.

4. **Incremental Processing**: Implementing `is_incremental()` pattern early enables efficient data processing as volumes grow.

---

## 📝 Next Steps (Future Considerations)

1. **Reintegrate Snapshots**: Once staging layer is stable, consider re-adding snapshot layer for full historical tracking
2. **Data Quality Tests**: Expand DBT tests for data validation
3. **Monitoring**: Add alerting for pipeline failures
4. **Performance Optimization**: Partition Delta tables for query performance
5. **Documentation**: Auto-generate DBT docs and integrate with Streamlit

---

## 📂 Key Files Modified/Created

### Modified
- `dbt_pro/my_dbt_project/models/core/dim_company.sql`
- `dbt_pro/my_dbt_project/models/staging/stg_customer.sql`
- `dags/tasks/transform_salesorder_to_silver.py`
- `dags/tasks/task2.py`

### Created
- `dags/tasks/delta_utils.py` (new utility module)

---

**Document Version:** 1.0  
**Last Updated:** March 20, 2026
