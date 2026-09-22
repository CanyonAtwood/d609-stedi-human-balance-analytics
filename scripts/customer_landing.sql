-- customer_landing.sql
-- STEDI Human Balance Analytics : Landing Zone DDL
-- Source : fulfillment / STEDI website customer records (JSON)
--
-- NOTE: replace stedi-canyon-2026 below with your own S3 bucket name before running.

CREATE DATABASE IF NOT EXISTS stedi;

CREATE EXTERNAL TABLE IF NOT EXISTS stedi.customer_landing (
    customername              string,
    email                     string,
    phone                     string,
    birthday                  string,
    serialnumber              string,
    registrationdate          bigint,
    lastupdatedate            bigint,
    sharewithresearchasofdate bigint,
    sharewithpublicasofdate   bigint,
    sharewithfriendsasofdate  bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'TRUE',
    'dots.in.keys'          = 'FALSE',
    'case.insensitive'      = 'TRUE'
)
STORED AS TEXTFILE
LOCATION 's3://stedi-canyon-2026/customer/landing/'
TBLPROPERTIES ('classification' = 'json');
