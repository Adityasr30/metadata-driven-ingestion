# Metadata-Driven Ingestion Framework (MDIF)

A configurable, reusable data ingestion framework built on **PySpark** that moves data across layers of a data lakehouse using a JSON-based configuration file — eliminating hardcoded pipeline logic.

---

## Overview

The MDIF (Metadata-Driven Ingestion Framework) is designed to handle multi-stage data ingestion in a lakehouse architecture. Instead of writing a separate pipeline for each dataset, all pipeline behaviour — source paths, target paths, transformations, schema validation, partitioning — is driven by a single JSON config file.

The framework supports two ingestion stages out of the box:

| Stage | Key | Description |
|---|---|---|
| Landing → Bronze | `landing_to_bronze` | Reads raw text/JSON files, adds batch metadata, writes as Delta tables |
| Bronze → Silver Staging | `bronze_to_silver_staging` | Flattens, validates, explodes, and renames data into analytics-ready Delta tables |

---

## Architecture

```
ADLS Gen2 - Landing Container
  └── PurchaseOrder/{year}/{month}/{day}/{batch_id}/{file}.json
        │
        ▼  [landing_to_bronze]
        │  • Read as stream (text format)
        │  • Add batch metadata (batch_year, batch_month, batch_day, batch_id, file_name)
        │  • Rename columns via config transformations
        │  • Write as Delta table (partitioned by batch metadata)
        │
ADLS Gen2 - Bronze Container
  └── purchase_orders_mdif/   →   bronze.purchase_orders_mdif
        │
        ▼  [bronze_to_silver_staging]
        │  • Read from Bronze Delta table
        │  • Flatten nested JSON (e.g. order_details__billing_address__country)
        │  • Explode array columns (e.g. order_details__items)
        │  • Validate schema against predefined schema
        │  • Rename columns (source_to_target_mapping)
        │  • Write as Delta table (partitioned by batch metadata)
        │
ADLS Gen2 - Silver Staging Container
  └── purchase_orders_mdif/   →   silver_staging.purchase_orders_mdif
        │
        ▼  [on schema mismatch]
        │
ADLS Gen2 - Silver Container
  └── error_table_mdif/       →   silver.error_table_mdif
```

---

## Features

| Feature | Description |
|---|---|
| Config-driven | All pipeline behaviour controlled via a JSON config file — no hardcoded logic |
| Multi-stage | Supports Landing→Bronze and Bronze→Silver Staging in a single notebook run |
| Dynamic transformations | Ordered `rename_column` and `add_column` (eval-based) actions per config |
| JSON flattening | Recursively flattens nested `StructType` columns with `__` separator and optional prefix removal |
| Column exploding | Explodes `ArrayType` columns (e.g. `order_details__items`) for row-level access |
| Column renaming | Source-to-target column mapping defined entirely in config |
| Schema validation | Validates incoming data against a predefined PySpark schema; handles missing and additional attributes |
| Error logging | Captures schema violations (error code `1002`) into a dedicated Delta error table |
| Streaming + batch | Supports both `readStream` and `read` modes, configurable per stage |
| Schema evolution | Writes with `mergeSchema=true` to handle schema changes over time |
| Configurable partitioning | Partition keys defined per target in config |

---

## Project Structure

```
metadata-driven-ingestion/
│
├── nb_bronze_mdif_ingestion.ipynb        # Main ingestion notebook
│
├── config/
│   └── purchase_orders/
│       ├── purchase_orders_config.json   # Pipeline configuration (source, target, transformations)
│       └── purchase_orders_schema.txt    # Predefined PySpark schema for validation
│
└── README.md
```

---

## Configuration File

All pipeline behaviour is defined in `purchase_orders_config.json`. It has two top-level stage keys.

### Stage 1: `landing_to_bronze`

**Source** reads raw text files from the Landing container using Spark Structured Streaming:

```json
"source": {
  "read_as": "stream",
  "data_layer": "landing",
  "storage_account": "<your-storage-account>",
  "container": "landing",
  "relative_path": "PurchaseOrder/*/*/*/*",
  "format": "text",
  "infer_schema": "infer",
  "type": "file",
  "multiline": true,
  "transformations": [
    { "order": 1, "action": "rename_column", "column_name": "value", "alias": "data" },
    { "order": 2, "action": "add_column", "column_name": "file_name", "tranformation_type": "eval",
      "transformation_expression": "split(input_file_name(), '/').getItem(8)" },
    { "order": 3, "action": "add_column", "column_name": "batch_id", "tranformation_type": "eval",
      "transformation_expression": "split(input_file_name(), '/').getItem(7)" },
    { "order": 4, "action": "add_column", "column_name": "batch_day", "tranformation_type": "eval",
      "transformation_expression": "split(input_file_name(), '/').getItem(6)" },
    { "order": 5, "action": "add_column", "column_name": "batch_month", "tranformation_type": "eval",
      "transformation_expression": "split(input_file_name(), '/').getItem(5)" },
    { "order": 6, "action": "add_column", "column_name": "batch_year", "tranformation_type": "eval",
      "transformation_expression": "split(input_file_name(), '/').getItem(4)" }
  ]
}
```

**Target** writes to the Bronze Delta table, partitioned by batch metadata:

```json
"target": {
  "targets": [{
    "storage_account": "<your-storage-account>",
    "container": "bronze",
    "relative_path": "purchase_orders_mdif",
    "format": "delta",
    "db": "bronze",
    "table": "purchase_orders_mdif",
    "mode": "append",
    "partition_cols": ["batch_year", "batch_month", "batch_day", "batch_id"],
    "source_to_target_mapping": {
      "data": "data",
      "batch_year": "batch_year",
      "batch_month": "batch_month",
      "batch_day": "batch_day",
      "batch_id": "batch_id",
      "file_name": "file_name"
    }
  }]
}
```

---

### Stage 2: `bronze_to_silver_staging`

**Source** reads from the Bronze Delta table as a stream:

```json
"source": {
  "read_as": "stream",
  "data_layer": "bronze",
  "storage_account": "<your-storage-account>",
  "container": "bronze",
  "relative_path": "purchase_orders",
  "format": "delta",
  "type": "table"
}
```

**Target** flattens, validates, explodes, and renames before writing to Silver Staging:

```json
"target": {
  "targets": [{
    "storage_account": "<your-storage-account>",
    "container": "silver-staging",
    "relative_path": "purchase_orders_mdif",
    "format": "delta",
    "db": "silver_staging",
    "table": "purchase_orders_mdif",
    "mode": "append",
    "validate_schema": true,
    "flatten": true,
    "prefix_remove": "data.",
    "explode_columns": ["order_details__items"],
    "partition_cols": ["batch_year", "batch_month", "batch_day", "batch_id"],
    "source_to_target_mapping": {
      "order_type": "order_type",
      "marketplace_order_id": "marketplace_order_id",
      "distributor": "distributor",
      "supplier_code": "supplier_code",
      "order_details__order_create_date": "order_create_date",
      "order_details__promised_delivery_date": "promised_delivery_date",
      "order_details__promised_shipment_date": "promised_shipment_date",
      "order_details__distributor_GLN": "distributor_gln",
      "order_details__billing_address__state": "billing_address_state",
      "order_details__billing_address__street": "billing_address_street",
      "order_details__billing_address__country": "billing_address_country",
      "order_details__billing_address__postcode": "billing_address_postcode",
      "order_details__delivery_address__country": "delivery_address_country",
      "order_details__delivery_address__address1": "delivery_address_address1",
      "order_details__delivery_address__address2": "delivery_address_address2",
      "order_details__delivery_address__postcode": "delivery_address_postcode",
      "order_details__items__gtin": "items_gtin",
      "order_details__items__unit_price_exl_tax": "items_unit_price_exl_tax",
      "order_details__items__qty": "items_qty",
      "products": "products",
      "batch_year": "batch_year",
      "batch_month": "batch_month",
      "batch_day": "batch_day",
      "batch_id": "batch_id",
      "file_name": "file_name"
    }
  }]
}
```

---

## Schema File

The predefined schema used for validation (`purchase_orders_schema.txt`) defines the expected structure of the incoming data:

```
Root
├── distributor                  (StringType)
├── marketplace_order_id         (StringType)
├── order_details
│   ├── billing_address
│   │   ├── country              (StringType)
│   │   ├── postcode             (StringType)
│   │   ├── state                (StringType)
│   │   └── street               (StringType)
│   ├── delivery_address
│   │   ├── address1             (StringType)
│   │   ├── address2             (StringType)
│   │   ├── country              (StringType)
│   │   └── postcode             (StringType)
│   ├── distributor_GLN          (StringType)
│   ├── items[]
│   │   ├── gtin                 (StringType)
│   │   ├── qty                  (StringType)
│   │   └── unit_price_exl_tax   (StringType)
│   ├── order_create_date        (StringType)
│   ├── promised_delivery_date   (StringType)
│   └── promised_shipment_date   (StringType)
├── order_type                   (StringType)
├── products[]                   (ArrayType - LongType)
├── supplier_code                (StringType)
├── batch_year                   (StringType)
├── batch_month                  (StringType)
├── batch_day                    (StringType)
├── batch_id                     (StringType)
└── file_name                    (StringType)
```

Any field present in the incoming data but absent from this schema is flagged as an error (`error_code: 1002`) and written to the error table. Any field missing from the incoming data is added as `null`.

---

## Silver Staging — Output Schema

After flattening and renaming, the Silver Staging table contains the following columns:

| Column | Source Field |
|---|---|
| `order_type` | `order_type` |
| `marketplace_order_id` | `marketplace_order_id` |
| `distributor` | `distributor` |
| `supplier_code` | `supplier_code` |
| `order_create_date` | `order_details.order_create_date` |
| `promised_delivery_date` | `order_details.promised_delivery_date` |
| `promised_shipment_date` | `order_details.promised_shipment_date` |
| `distributor_gln` | `order_details.distributor_GLN` |
| `billing_address_state` | `order_details.billing_address.state` |
| `billing_address_street` | `order_details.billing_address.street` |
| `billing_address_country` | `order_details.billing_address.country` |
| `billing_address_postcode` | `order_details.billing_address.postcode` |
| `delivery_address_country` | `order_details.delivery_address.country` |
| `delivery_address_address1` | `order_details.delivery_address.address1` |
| `delivery_address_address2` | `order_details.delivery_address.address2` |
| `delivery_address_postcode` | `order_details.delivery_address.postcode` |
| `items_gtin` | `order_details.items[].gtin` |
| `items_qty` | `order_details.items[].qty` |
| `items_unit_price_exl_tax` | `order_details.items[].unit_price_exl_tax` |
| `products` | `products` |
| `batch_year` | Extracted from file path |
| `batch_month` | Extracted from file path |
| `batch_day` | Extracted from file path |
| `batch_id` | Extracted from file path |
| `file_name` | Extracted from file path |

---

## Error Table Schema

Schema violations are written to `silver.error_table_mdif`:

| Column | Type | Description |
|---|---|---|
| `error_column` | string | Column that triggered the error |
| `error_code` | int | `1002` = additional attribute received |
| `error_description` | string | Human-readable description |
| `batch_id` | string | Batch identifier |
| `file_name` | string | Source file name |
| `error_data` | string | Actual value that failed validation |
| `created_ts` | timestamp | Record creation time |
| `updated_ts` | timestamp | Record last updated time |

---

## Core Functions

| Function | Description |
|---|---|
| `run_mdif(config, stage)` | Main orchestrator — reads config, ingests, transforms, and writes data for a given stage |
| `transform_df(df, transformations)` | Applies ordered transformations (rename/add columns) from config |
| `flatten_df(df, prefix_remove)` | Recursively flattens nested `StructType` columns, joining names with `__` |
| `explode_columns(df, columns)` | Explodes `ArrayType` columns and flattens `StructType` columns |
| `rename_columns(df, col_map)` | Renames columns based on source-to-target dictionary |
| `validate_schema(df)` | Compares DataFrame schema against predefined schema; logs errors and adds missing fields as null |
| `write_df_to_target(...)` | Writes DataFrame to Delta table with streaming, partitioning, and schema merging |
| `create_error_table()` | Creates the Delta error table if it does not already exist |

---

## Prerequisites

- Azure Synapse Analytics or Databricks (PySpark environment)
- Azure Data Lake Storage Gen2
- Delta Lake
- `notebookutils` (available natively in Azure Synapse and Databricks)

---

## Getting Started

1. Clone this repository.
2. Upload `purchase_orders_config.json` and `purchase_orders_schema.txt` to your ADLS Gen2 `config` container.
3. Update the parameters in the notebook:

```python
storage_account_name      = "<your-storage-account-name>"
mdif_config_relative_path = "purchase_orders/purchase_orders_config.json"
mdif_schema_relative_path = "purchase_orders/purchase_orders_schema.txt"
batch_id                  = "<your-batch-id>"
```

4. Run the notebook — it executes both stages sequentially:

```python
run_mdif(config_df, mdif_bronze_stage)   # Landing → Bronze
run_mdif(config_df, mdif_silver_stage)   # Bronze → Silver Staging
```

---

## Extending to a New Dataset

To onboard a new dataset, no code changes are needed. Simply:

1. Create a new config JSON following the same structure as `purchase_orders_config.json`.
2. Create a new schema `.txt` file with the expected PySpark `StructType`.
3. Upload both to the config container.
4. Point the notebook parameters to the new config and schema files.

---

## Tech Stack

![PySpark](https://img.shields.io/badge/PySpark-3.x-orange)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-enabled-green)
![Azure ADLS](https://img.shields.io/badge/Azure-ADLS%20Gen2-blue)
![Azure Synapse](https://img.shields.io/badge/Azure-Synapse%20Analytics-blue)
![Structured Streaming](https://img.shields.io/badge/Spark-Structured%20Streaming-red)
