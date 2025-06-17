# Rental Marketplace Batch ETL Pipeline

This project implements a **production-grade batch ETL data pipeline** for a rental marketplace platform (similar to Airbnb), built using **AWS Glue**, **Amazon S3**, **Amazon Redshift**, **AWS Step Functions**, and **AWS Aurora MySQL**.

---

## Project Overview

The platform captures data in an **Aurora MySQL database** from user interactions, listings, and bookings. This pipeline transforms the data into analytics-ready formats in Amazon Redshift for dashboarding, reporting, and business insights.

---

## Project Structure

```
/glue_jobs/
  ├── extract_to_s3.py
  ├── load_to_redshift.py
  ├── transform_curated.py
  └── generate_kpis.py

/step_function/
  └── etl_workflow_definition.json

/sql/
  ├── auroraTables.sql 
  ├── redshift_presentation.sql
  ├── redshift_rawsTables.sql
  └── redshift_curatedTables.sql

/docs/
  ├── /project_images/
  └── architecture_diagram.png

README.md
```

---

## Architecture Overview

```
Aurora MySQL
     ↓
[AWS Glue Job 1] - Extract & Load to S3 (raw)
     ↓
Amazon S3 (raw zone)
     ↓
[AWS Glue Job 2] - Load to Redshift raw layer
     ↓
Amazon Redshift (raw schema)
     ↓
[AWS Glue Job 3] - Transform to curated layer
     ↓
Amazon Redshift (curated schema)
     ↓
[AWS Glue Job 4] - Compute KPIs
     ↓
Amazon Redshift (presentation schema)
     ↓
[AWS Step Functions] - Orchestrates All Glue Jobs w/ Retry + Failover
```

---

## Data Sources

| Table                  | Description                                                  |
| ---------------------- | ------------------------------------------------------------ |
| `apartment_attributes` | Apartment metadata including features, location, and fees    |
| `apartments`           | Listing info: title, source, price, created date, is\_active |
| `bookings`             | Booking transactions: dates, status, user, apartment         |
| `user_viewing`         | User interaction logs (wishlist, view timestamp, actions)    |

---

## Components Used

| AWS Service         | Purpose                                    |
| ------------------- | ------------------------------------------ |
| **Aurora MySQL**    | Source of truth for application data       |
| **Amazon S3**       | Landing zone for extracted raw data        |
| **AWS Glue**        | ETL transformations and Redshift ingestion |
| **Amazon Redshift** | Analytical data warehouse                  |
| **Step Functions**  | Orchestration of ETL steps                 |
| **CloudWatch Logs** | Logging for Glue and Step Functions        |

---

## Pipeline Flow & Glue Jobs

### Glue Job 1: `aurora-s3-raw-dataLoad`

* Extracts raw data from Aurora MySQL
* Writes to S3 as Parquet in `/raw/` folder

### Glue Job 2: `s3-redshift-raw-dataLoad`

* Loads raw S3 data into Redshift `raw` schema
* No transformation; format-preserving

### Glue Job 3: `raw-curated-transformation`

* Casts strings to appropriate types (DATE, BOOLEAN, DECIMAL)
* Filters for valid data (e.g., `confirmed` bookings, `is_active = TRUE`)
* Loads into `curated` schema

### Glue Job 4: `curated-presentation-kpisComputation`

* Computes KPIs and stores in `presentation` schema

---

## Key KPIs Computed

| KPI                          | Description                                               |
| ---------------------------- | --------------------------------------------------------- |
| Weekly Average Listing Price | Avg price of active listings per week                     |
| Repeat Customer Rate         | Users who book more than once in a rolling 30-day window  |
| Most Popular Locations       | Cities with the most bookings                             |
| Occupancy Rate               | Estimated as `booked nights / available nights` per month |
| Average Booking Duration     | Avg stay duration in days                                 |
| Top Performing Listings      | Apartments with the highest revenue                       |

---

## Step Function Orchestration

All Glue jobs are orchestrated using a Step Function with:

* **Retry policies with exponential backoff**
* **Error handling via Catch blocks**
* **Failover to error handler (FailState or SNS notification)**

### Step Function States:

```json
"StartAt": "ExtractToS3",
"States": {
  "ExtractToS3": { ... },
  "LoadToRedshift": { ... },
  "TransformCurated": { ... },
  "GenerateKPIs": { ... }
}
```

---

## Redshift Schema Structure

### `raw` Layer (unchanged from source)

* `raw.bookings`
* `raw.apartments`
* `raw.apartment_attributes`
* `raw.user_viewing`

### `curated` Layer (cleaned & typed)

* `curated.bookings`
* `curated.apartments`
* `curated.apartment_attributes`
* `curated.user_viewing`

### `presentation` Layer (aggregates)

* `presentation.weekly_avg_price`
* `presentation.repeat_customers`
* `presentation.popular_locations`
* `presentation.occupancy_rate`
* `presentation.avg_booking_duration`
* `presentation.top_performing_listings`

---

## Error Handling & Best Practices

*  **Retries**: All jobs have 3 retries with exponential backoff
*  **Catch Blocks**: Fail gracefully and trigger `FailState`
*  **Parameterization**: Glue jobs can accept `--run_date` or dynamic arguments
*  **S3 Temporary Directory** for Redshift writes
*  **Data Type Validation**: Boolean, date, numeric casting in curated layer
*  **Partitioning Option**: Performance tuning for large data volumes (future-ready)

---

## How to Run the Pipeline

1. Make sure all Redshift tables (raw/curated/presentation) exist
2. Trigger the Step Function from AWS Console or CLI
3. Monitor execution in Step Functions or CloudWatch
4. Query KPIs from Redshift presentation schema

---

## Example SQL for Validating KPIs in Redshift

```sql
-- Check average listing price
SELECT * FROM presentation.weekly_avg_price ORDER BY week DESC;

-- Find repeat customers
SELECT * FROM presentation.repeat_customers WHERE is_repeat_customer = 1;

-- Occupancy trends
SELECT * FROM presentation.occupancy_rate ORDER BY month DESC;
```
---

## Author

**Peter Caleb Ayertey**
---
