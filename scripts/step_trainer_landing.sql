-- step_trainer_landing.sql
-- STEDI Human Balance Analytics : Landing Zone DDL
-- Source : STEDI Step Trainer motion sensor records (JSON)
--
-- NOTE: replace <YOUR-BUCKET> below with your own S3 bucket name before running.

CREATE DATABASE IF NOT EXISTS stedi;

CREATE EXTERNAL TABLE IF NOT EXISTS stedi.step_trainer_landing (
    sensorreadingtime  bigint,
    serialnumber       string,
    distancefromobject int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'TRUE',
    'dots.in.keys'          = 'FALSE',
    'case.insensitive'      = 'TRUE'
)
STORED AS TEXTFILE
LOCATION 's3://<YOUR-BUCKET>/step_trainer/landing/'
TBLPROPERTIES ('classification' = 'json');
