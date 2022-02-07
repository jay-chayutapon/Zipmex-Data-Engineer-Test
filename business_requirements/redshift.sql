--PostgreSQL Federated Query
CREATE EXTERNAL SCHEMA postgres_zipmex
FROM POSTGRES
DATABASE 'postgres' SCHEMA 'public'
URI 'postgresql-zipmex.cjovdars256t.ap-southeast-1.rds.amazonaws.com' PORT 5432
IAM_ROLE 'arn:aws:iam::748544143050:role/FullAccessForRedShift'
SECRET_ARN 'arn:aws:secretsmanager:ap-southeast-1:748544143050:secret:zipmex_postgres_secret-eFOKXb';

--Create Glue Data Catalog External Schema
CREATE EXTERNAL SCHEMA glue_schema_zipmex
FROM DATA CATALOG
DATABASE 'glue_database_zipmex'
REGION 'ap-southeast-1'
IAM_ROLE 'arn:aws:iam::748544143050:role/FullAccessForRedShift';

--Order Detail External Table
CREATE EXTERNAL TABLE glue_schema_zipmex.order_detail
PARTITIONED BY (dt)
STORED AS parquet
LOCATION 's3://zipmex-de-test/ETL/order_detail_partitioned_by_YYYYMMDD'
AS 
 SELECT 
  order_created_timestamp,
  TO_CHAR(order_created_timestamp, 'YYYYMMDD') AS dt,
  status, 
  price, 
  discount, 
  id, 
  driver_id, 
  user_id, 
  restaurant_id
 FROM postgres_zipmex.order_detail;
  	
--Restaurant Detail External Table
CREATE EXTERNAL TABLE glue_schema_zipmex.restaurant_detail
PARTITIONED BY (dt)
STORED AS parquet
LOCATION 's3://zipmex-de-test/ETL/restaurant_detail_partitioned_by_static_value'
AS 
 SELECT CURRENT_DATE as dt,
  id,
  restaurant_name,
  category,
  esimated_cooking_time,
  latitude,
  longitude
 FROM postgres_zipmex.restaurant_detail;
 
--Order Detail New External Table
CREATE EXTERNAL TABLE glue_schema_zipmex.order_detail_new
PARTITIONED BY (dt)
STORED AS parquet
LOCATION 's3://zipmex-de-test/ETL/order_detail_new_partitioned_by_YYYYMMDD'
AS 
 SELECT 
  order_created_timestamp,
  dt,
  status, 
  price,
  discount,
  CASE
   WHEN discount IS NULL THEN 0
   ELSE discount
  END AS discount_no_null,
  id, 
  driver_id, 
  user_id, 
  restaurant_id 
FROM glue_schema_zipmex.order_detail;

--Restaurant Detail New External Table
CREATE EXTERNAL TABLE glue_schema_zipmex.restaurant_detail_new
PARTITIONED BY (dt)
STORED AS parquet
LOCATION 's3://zipmex-de-test/ETL/restaurant_detail_new_partitioned_by_static_value'
AS 
 SELECT 
  dt,
  id,
  restaurant_name,
  category,
  esimated_cooking_time,
  CASE
   WHEN esimated_cooking_time <= 40 THEN 1
   WHEN esimated_cooking_time BETWEEN 41 AND 80 THEN 2
   WHEN esimated_cooking_time BETWEEN 81 AND 120 THEN 3
   WHEN esimated_cooking_time > 120 THEN 4
  END AS cooking_bin,
  latitude,
  longitude
 FROM glue_schema_zipmex.restaurant_detail;