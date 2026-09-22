-- accelerometer_landing.sql
-- STEDI Human Balance Analytics : Landing Zone DDL
-- Source : mobile app accelerometer readings (JSON)
--
-- NOTE: replace stedi-canyon-2026 below with your own S3 bucket name before running.
-- The JSON key is "timestamp" (lowercase) and "user" holds the customer email.

CREATE DATABASE IF NOT EXISTS stedi;

CREATE EXTERNAL TABLE IF NOT EXISTS stedi.accelerometer_landing (
    user      string,
    timestamp bigint,
    x         double,
    y         double,
    z         double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'TRUE',
    'dots.in.keys'          = 'FALSE',
    'case.insensitive'      = 'TRUE'
)
STORED AS TEXTFILE
LOCATION 's3://stedi-canyon-2026/accelerometer/landing/'
TBLPROPERTIES ('classification' = 'json');
