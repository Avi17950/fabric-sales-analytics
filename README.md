# Sales Analytics Pipeline on Microsoft Fabric

An end-to-end analytics solution on Microsoft Fabric: raw CSV through a
medallion architecture, into a star schema, surfaced as a Power BI semantic
model and report, and distributed through a Fabric app.

---

## Overview

| | |
|---|---|
| **Platform** | Microsoft Fabric |
| **Storage** | OneLake — Lakehouse and Warehouse |
| **Processing** | PySpark notebooks, T-SQL |
| **Modelling** | Star schema — 1 fact, 4 dimensions |
| **Semantic layer** | Power BI semantic model with DAX measures |
| **Reporting** | Power BI report, 4 pages |
| **Distribution** | Fabric app |

---

## Architecture

```
CSV source
    │
    ▼
┌──────────────────────────────────────────────┐
│  LAKEHOUSE  (lh_sales)                       │
│                                              │
│  Files/raw/sales_data.csv                    │
│      │                                       │
│      ▼                                       │
│  BRONZE   dbo.bronze_sales                   │
│      │    raw ingest, untouched              │
│      ▼                                       │
│  SILVER   dbo.silver_sales                   │
│      │    typed, cleaned, filtered, deduped  │
│      ▼                                       │
│  GOLD     dbo.fact_sales                     │
│           dbo.dim_date                       │
│           dbo.dim_region                     │
│           dbo.dim_customer                   │
│           dbo.dim_product                    │
└──────────────────────────────────────────────┘
    │
    ▼
Semantic model  ──►  Power BI report  ──►  Fabric app
```

---

## Bronze — raw ingest

The source CSV is landed in the lakehouse `Files` area and written to Delta
with no transformation. Column names, types and bad rows are preserved as
received, so the raw state can always be reproduced.

**Columns as received:** `OrderID`, `OrderDate`, `CustomerName`, `Region`,
`ProductCategory`, `Revenue`, `Quantity`, `Status`

```python
df_bronze = (
    spark.read.format("csv")
         .option("header", "true")
         .option("inferSchema", "true")
         .load("Files/raw/sales_data.csv")
)

df_bronze.write.format("delta").mode("overwrite").saveAsTable("dbo.bronze_sales")
```

---

## Silver — cleaned and conformed

1. Column names standardised to `snake_case`
2. `order_date` parsed from text (`dd-MM-yyyy`) into a true `date`
3. `revenue` cast to `decimal(18,2)`, `quantity` cast to `int`
4. Whitespace trimmed from text columns
5. Business filter: `status = 'Active'` and `revenue > 0`
6. Duplicates removed on `order_id`
7. Derived columns: `order_year`, `order_month`, `year_month`
8. Audit column `processed_at`

```python
df_silver = (
    df_typed
      .filter((F.upper(F.col("status")) == "ACTIVE") & (F.col("revenue") > 0))
      .dropDuplicates(["order_id"])
      .withColumn("year_month", F.date_format("order_date", "yyyy-MM"))
      .withColumn("processed_at", F.current_timestamp())
)
```

---

## Gold — star schema

**Fact — `dbo.fact_sales`**

| Column | Type | Role |
|---|---|---|
| order_id | string | Degenerate dimension |
| date_key | int | FK → dim_date (`yyyyMMdd`) |
| region_key | int | FK → dim_region |
| customer_key | int | FK → dim_customer |
| product_key | int | FK → dim_product |
| quantity | int | Measure |
| revenue | decimal(18,2) | Measure |

**Dimensions**

| Table | Grain | Key |
|---|---|---|
| dim_date | one row per order date | `date_key` (`yyyyMMdd`) |
| dim_region | one row per region | `region_key` (surrogate) |
| dim_customer | one row per customer | `customer_key` (surrogate) |
| dim_product | one row per product category | `product_key` (surrogate) |

Surrogate keys use `row_number()` rather than `monotonically_increasing_id()`,
which produces large non-sequential values because it encodes the Spark
partition into the key.

```python
dim_region = (
    df.select(F.col("Region").alias("region")).distinct()
      .withColumn("region_key", F.row_number().over(Window.orderBy("region")))
      .select("region_key", "region")
)
```

---

## Semantic model

The semantic model sits directly on the gold Delta tables using **Direct Lake**,
Fabric's storage mode that reads Parquet from OneLake without importing a copy
or issuing a query per visual. No scheduled refresh is required: new data
written by the pipeline appears in the report on the next reframe.

### Relationships

| From (fact) | To (dimension) | Cardinality | Filter direction |
|---|---|---|---|
| fact_sales[date_key] | dim_date[date_key] | Many-to-one | Single |
| fact_sales[region_key] | dim_region[region_key] | Many-to-one | Single |
| fact_sales[customer_key] | dim_customer[customer_key] | Many-to-one | Single |
| fact_sales[product_key] | dim_product[product_key] | Many-to-one | Single |

All relationships are single-direction, from dimension to fact. Bi-directional
filtering was deliberately avoided: it introduces ambiguous filter paths once
more than one dimension is involved, and is rarely needed in a clean star.

### Date table configuration

`dim_date` is marked as the model's official date table, which enables the
DAX time-intelligence functions (`DATEADD`, `TOTALYTD`, `SAMEPERIODLASTYEAR`)
to work without manual date arithmetic.

### Model housekeeping

- Surrogate key columns hidden from the report view, since they carry no
  business meaning
- `revenue` formatted as currency, `quantity` as whole number with thousands
  separator
- `dim_date[month_name]` sorted by `dim_date[month]` so months appear in
  calendar order rather than alphabetically
- Summarisation set to **Do not summarize** on all key and text columns, which
  stops Power BI silently offering to sum an ID

### DAX measures

Measures were written rather than relying on implicit aggregation, so the
logic lives in one place and is reusable across visuals.

```dax
Total Revenue =
SUM ( fact_sales[revenue] )

Total Orders =
COUNTROWS ( fact_sales )

Total Quantity =
SUM ( fact_sales[quantity] )

Avg Order Value =
DIVIDE ( [Total Revenue], [Total Orders] )

Unique Customers =
DISTINCTCOUNT ( fact_sales[customer_key] )

Revenue PM =                                    -- previous month
CALCULATE ( [Total Revenue], DATEADD ( dim_date[order_date], -1, MONTH ) )

Revenue MoM % =
DIVIDE ( [Total Revenue] - [Revenue PM], [Revenue PM] )

Revenue YTD =
TOTALYTD ( [Total Revenue], dim_date[order_date] )

Revenue Rank by Region =
RANKX ( ALL ( dim_region[region] ), [Total Revenue],, DESC )

Revenue % of Total =
DIVIDE ( [Total Revenue], CALCULATE ( [Total Revenue], ALL ( fact_sales ) ) )
```

`DIVIDE` is used throughout rather than the `/` operator, because it returns
blank on a zero denominator instead of an error, which matters on the first
month where there is no prior period.

---

## Report

Four pages, each answering a different question.

### Page 1 — Executive overview

- KPI cards: Total Revenue, Total Orders, Avg Order Value, Unique Customers
- Line chart: revenue by `year_month` with a trend line
- Card with MoM % change, conditionally formatted green or red
- Slicers: year, region, product category

### Page 2 — Regional performance

- Clustered bar: revenue by region
- Matrix: region on rows, month on columns, revenue as values
- Table: region, revenue, orders, avg order value, rank

### Page 3 — Product analysis

- Donut: revenue share by product category
- Stacked column: revenue by category over time
- Scatter: quantity against revenue, one point per category

### Page 4 — Customer insight

- Top N visual: top 10 customers by revenue
- Table with revenue, order count and average order value per customer
- Distinct customer count trended by month

### Report design decisions

- A consistent colour palette applied through a custom theme JSON rather than
  per-visual formatting
- Cross-filtering enabled between visuals on a page, so clicking a region
  filters the rest
- Tooltips carry the measure definitions, so a reader can see what a number
  means without opening the model
- Numbers abbreviated (₹1.2M rather than ₹1,234,567) on cards, full precision
  retained in tables

---

## Distribution via Fabric app

The report is published through a **Fabric app**, which packages selected
items from the workspace into a single destination for business users.

Why an app rather than sharing the report directly:

1. Consumers see only the app, not the workspace, so notebooks, lakehouse
   tables and work-in-progress stay hidden
2. Permissions are managed once at app level instead of per item
3. Navigation can be curated, with pages grouped and ordered
4. The workspace can keep changing without affecting consumers until the app
   is republished

**Audience setup**

| Audience | Sees | Permission |
|---|---|---|
| Sales leadership | All four pages | Read |
| Regional managers | Overview + Regional pages | Read |

---

## Data quality checks

Validation runs at the end of the pipeline rather than being assumed:

```python
# The fact join must not add or lose rows
assert silver_rows == fact_rows

# No unmatched dimension keys
for k in ["date_key", "region_key", "customer_key", "product_key"]:
    assert fact.filter(F.col(k).isNull()).count() == 0

# Revenue must reconcile between layers
assert round(silver_revenue, 2) == round(fact_revenue, 2)
```

A fact count higher than silver indicates duplicate keys in a dimension
causing join fan-out. A revenue mismatch indicates rows lost to a failed join.

---

## Problems solved along the way

**Date stored as text.** `OrderDate` arrived as a string, so `FORMAT()` in
T-SQL and date functions in Spark both rejected it. Converted once in silver
rather than repeating `CONVERT` in every downstream query.

**Join fan-out in the fact build.** The customer dimension initially carried a
`region` column, so a customer appearing in two regions received two surrogate
keys and duplicated their sales rows. Fixed by reducing each dimension to its
own grain.

**Lakehouse schema resolution.** Writes failed with `SCHEMA_NOT_FOUND` because
the lakehouse name was being passed into `saveAsTable`. The attached default
lakehouse already supplies it, so the name takes the form `schema.table` only.

**Python built-in shadowing.** `from pyspark.sql.functions import sum, count`
overwrites Python's own `sum` and `count`, producing a confusing `TypeError`.
Standardised on `from pyspark.sql import functions as F`.

**Months sorting alphabetically.** April led the axis instead of January, until
`month_name` was given a **Sort by column** of the numeric `month`.

---

## Repository contents

| Path | Description |
|---|---|
| `notebooks/01_bronze_ingest.ipynb` | CSV to bronze Delta table |
| `notebooks/02_silver_transform.ipynb` | Cleaning, typing, filtering |
| `notebooks/03_gold_aggregate.ipynb` | Monthly aggregations |
| `notebooks/04_star_schema.ipynb` | Fact and dimension build |
| `sql/` | Warehouse view and stored procedure |
| `dax/measures.dax` | All semantic model measures |
| `data/sales_data.csv` | Sample source data |
| `images/` | Architecture, model and report screenshots |

---

## Tech stack

`Microsoft Fabric` · `OneLake` · `Delta Lake` · `PySpark` · `Spark SQL` ·
`T-SQL` · `Power BI` · `DAX` · `Direct Lake` · `Medallion Architecture` ·
`Dimensional Modelling`
