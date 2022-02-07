CREATE TABLE public.order_detail (
	order_created_timestamp timestamp NULL,
	status varchar NULL,
	price int4 NULL,
	discount float8 NULL,
	id varchar NULL,
	driver_id varchar NULL,
	user_id varchar NULL,
	restaurant_id varchar NULL
);

CREATE TABLE public.restaurant_detail (
	id varchar NULL,
	restaurant_name varchar NULL,
	category varchar NULL,
	esimated_cooking_time float8 NULL,
	latitude float8 NULL,
	longitude float8 NULL
);


-- Install the required PostgreSQL extensions
CREATE EXTENSION aws_s3 CASCADE;

-- Import S3 data into Amazon RDS PostgreSQL
SELECT aws_s3.table_import_from_s3(
  'public.order_detail', -- define the target table
  'order_created_timestamp,status,price,discount,id,driver_id,user_id,restaurant_id', -- define the target table columns 
  '(format csv, header true)', -- define the source format
  'zipmex-de-test', -- define the source S3 bucket name
  'order_detail.csv', -- define the source file
  'ap-southeast-1', -- define timezone
  'xxxxxxxxxxxxx', -- define AWS access key (To allow RDS access to S3) Please use the key given in the Google Slide
  'xxxxxxxxxxxxx' -- define AWS secret key (To allow RDS access to S3) Please use the key given in the Google Slide
);

SELECT aws_s3.table_import_from_s3(
  'public.restaurant_detail', -- define the target table
  'id,restaurant_name,category,esimated_cooking_time,latitude,longitude', -- define the target table columns 
  '(format csv, header true)', -- define the source format
  'zipmex-de-test', -- define the source S3 bucket name
  'restaurant_detail.csv', -- define the source file
  'ap-southeast-1', -- define timezone
  'xxxxxxxxxxxxxxxxx', -- define AWS access key (To allow RDS access to S3) Please use the key given in the Google Slide
  'xxxxxxxxxxxxxxxxx' -- define AWS secret key (To allow RDS access to S3) Please use the key given in the Google Slide
);
