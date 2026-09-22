# Athena Query Screenshots

Row counts verified against the project rubric's "Check your work!" targets.

## Landing Zone

| Screenshot | Query | Rows | Target |
|---|---|---|---|
| `customer_landing.png` | `count(*) FROM stedi.customer_landing` | 956 | 956 |
| `accelerometer_landing.png` | `count(*) FROM stedi.accelerometer_landing` | 81,273 | 81273 |
| `step_trainer_landing.png` | `count(*) FROM stedi.step_trainer_landing` | 28,680 | 28680 |
| `customer_landing_blank_consent.png` | `count(*) ... WHERE sharewithresearchasofdate IS NULL` | 474 | multiple rows |

## Trusted Zone

| Screenshot | Query | Rows | Target |
|---|---|---|---|
| `customer_trusted.png` | `count(*) FROM stedi.customer_trusted` | 482 | 482 |
| `customer_trusted_no_blank_consent.png` | `count(*) ... WHERE sharewithresearchasofdate IS NULL` | 0 | no blank rows |
| `accelerometer_trusted.png` | `count(*) FROM stedi.accelerometer_trusted` | 40,981 | 40981 |
| `step_trainer_trusted.png` | `count(*) FROM stedi.step_trainer_trusted` | 14,460 | 14460 |

## Curated Zone

| Screenshot | Query | Rows | Target |
|---|---|---|---|
| `customer_curated.png` | `count(*) FROM stedi.customer_curated` | 482 | 482 |
| `machine_learning_curated.png` | `count(*) FROM stedi.machine_learning_curated` | 43,681 | 43681 |

## Supporting

`glue_job_runs_summary.png` - AWS Glue job run monitoring: 5 runs, 5 succeeded,
0 failed, 100% success rate.

## Note on the landing blank-consent count

474 customer records have no `sharewithresearchasofdate` value. 956 - 474 = 482,
which is exactly the `customer_trusted` row count, confirming the consent filter
drops every non-consenting record and keeps every consenting one.
