# STEDI Human Balance Analytics

Data lakehouse solution built for the STEDI Step Trainer, completed for
WGU D609 (Data Analytics at Scale) / Udacity "Spark and Data Lakes".

## Problem

The STEDI Step Trainer is a motion sensor that records the distance of a
detected object. A companion mobile app records the phone accelerometer on the
X, Y and Z axes. The data science team wants to train a model that detects
steps in real time, but the raw data is not usable yet:

- It arrives in three disconnected feeds.
- Only some customers consented to share their data for research, and only
  those customers' data may be used for training.
- The fulfillment website had a defect and reused the same 30 serial numbers
  across millions of customer records, so a customer's serial number is not a
  unique identifier for their device.

## Architecture

Three zones in S3, queried through the Glue Data Catalog and Athena.

```
                 landing                trusted                    curated
customer     customer_landing  -->  customer_trusted      -->  customer_curated
                (956)                   (482)                       (482)
accelerometer  accelerometer_landing --> accelerometer_trusted
                (81,273)                 (40,981)
step_trainer  step_trainer_landing --> step_trainer_trusted --> machine_learning_curated
                (28,680)                 (14,460)                   (43,681)
```

Join keys:

| From | To | Key |
|---|---|---|
| accelerometer | customer | `user` = `email` |
| step_trainer | customer | `serialnumber` |
| step_trainer | accelerometer | `sensorreadingtime` = `timestamp` |

## Contents

### SQL DDL (landing zone tables, created manually in the Glue console)

- `scripts/customer_landing.sql`
- `scripts/accelerometer_landing.sql`
- `scripts/step_trainer_landing.sql`

### Glue jobs

| Script | Output table | Rows |
|---|---|---|
| `scripts/customer_landing_to_trusted.py` | `customer_trusted` | 482 |
| `scripts/accelerometer_landing_to_trusted.py` | `accelerometer_trusted` | 40,981 |
| `scripts/customer_trusted_to_curated.py` | `customer_curated` | 482 |
| `scripts/step_trainer_trusted.py` | `step_trainer_trusted` | 14,460 |
| `scripts/machine_learning_curated.py` | `machine_learning_curated` | 43,681 |

### Screenshots

Athena query results for each zone are in `screenshots/`.

## Notes

- All scripts target the S3 bucket `stedi-canyon-2026`.
- The jobs use `Transform - SQL Query` nodes rather than Join nodes, and
  `Data Source - Data Catalog` nodes rather than S3 bucket nodes.
- Every sink sets `enableUpdateCatalog=True` and
  `updateBehavior="UPDATE_IN_DATABASE"` so the Glue table schema is inferred
  and updated on each run.
- Glue jobs append rather than replace. Delete the S3 output files and drop the
  Athena table before re-running a job, or row counts will be wrong.
