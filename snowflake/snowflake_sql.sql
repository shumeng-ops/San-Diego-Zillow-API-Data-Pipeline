-- create database and schema for transformed zillow data
CREATE DATABASE zillow_database;
CREATE SCHEMA zillow_schema;

-- create table to stored transformed zillow data
CREATE OR REPLACE TABLE zillow_database.zillow_schema.zillow_san_diego(
zpid STRING,
latitude FLOAT,
longitude FLOAT,
address STRING,
zipcode STRING,
cities STRING,
bedrooms INT,
bathrooms FLOAT,
livingarea INT,
yearBuilt INT,
lot_size FLOAT,
lot_size_unit STRING,
propertype STRING,
listingstatus STRING,
price INT,
pricePerSquareFoot INT,
zestimate INT,
estimated_price INT,
estimated_price_sqft FLOAT
);


--create file format which used for create tables from external S3
CREATE OR REPLACE FILE FORMAT zillow_database.zillow_schema.format_csv
    type='CSV'
    field_delimiter =','
    record_delimiter = '\n'
    skip_header=1
    FIELD_OPTIONALLY_ENCLOSED_BY = '0x22'

CREATE OR REPLACE STAGE zillow_schema.zillow_ext_stage
    url='s3://zillow-transform-data-bucket/'
    credentials=(
    aws_key_id='XXXXXXX'
    aws_secret_key='YYYYYYYY'
    )
    file_format = zillow_database.zillow_schema.format_csv;

list@zillow_database.zillow_schema.zillow_ext_stage;



---snowpipe create pipe
CREATE OR REPLACE PIPE zillow_database.zillow_schema.zillow_snowpipe
auto_ingest = TRUE
as 
COPY INTO zillow_database.zillow_schema.zillow_san_diego
from @zillow_database.zillow_schema.zillow_ext_stage;


desc pipe zillow_database.zillow_schema.zillow_snowpipe;


-----Create a Separate schema for analytics purposes
CREATE OR REPLACE SCHEMA zillow_analytics;

create or replace table zillow_database.zillow_analytics.sd_zoo_data as (
SELECT *  
FROM zillow_database.zillow_schema.zillow_san_diego 
WHERE PROPERTYPE IN ('singleFamily','townhome')
AND PRICEPERSQUAREFOOT<=10000);

;
