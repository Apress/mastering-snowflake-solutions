CREATE STAGE "BUILDING_SS"."PUBLIC".S3_STAGE 
URL = s3://building-solutions-with-snowflake' 
CREDENTIALS = (AWS_KEY_ID = '**********' 
               AWS_SECRET_KEY = '****************************************');
               
CREATE [ OR REPLACE ] FILE FORMAT [ IF NOT EXISTS ] <name>
                      TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML } [ formatTypeOptions ]
                      [ COMMENT = '<string_literal>' ];

COPY INTO [<namespace>.]<table_name>
     FROM { internalStage | externalStage | externalLocation }
[ FILES = ( '<file_name>' [ , '<file_name>' ] [ , ... ] ) ]
[ PATTERN = '<regex_pattern>' ]
[ FILE_FORMAT = ( { FORMAT_NAME = '[<namespace>.]<file_format_name>' |
                    TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML };
                 
                 
//Here is an example of selecting a subset of columns using column position:
COPY INTO home_sales(city, zip, sale_date, price)
   FROM (SELECT t.$1, t.$2, t.$6, t.$7 FROM @mystage/sales.csv.gz t)
   file_format = (format_name = mycsvformat);

//This example demonstrates how to apply a substring function, carry out concatenation and re-order columns:
COPY INTO HOME_SALES(city, zip, sale_date, price, full_name)
   FROM (SELECT substr(t.$2,4), t.$1, t.$5, t.$4, concat(t.$7, t.$8) from @mystage t)
   file_format = (format_name = mycsvformat);
                 
//Unload data
COPY INTO @ext_stage/result/data_ 
FROM 
  (
  SELECT t1.column_a, t1.column_b, t2.column_c 
  FROM table_one t1
  Inner join table_two t2 on t1.id = t2.id
  WHERE t2.column_c = '2018-04-29' 
  )

                 
//Task Syntax
CREATE [ OR REPLACE ] TASK [ IF NOT EXISTS ] <name>
  WAREHOUSE = <string>
  [ SCHEDULE = '{ <num> MINUTE | USING CRON <expr> <time_zone> }' ]
  [ ALLOW_OVERLAPPING_EXECUTION = TRUE | FALSE ]
  [ <session_parameter> = <value> [ , <session_parameter> = <value> ... ] ]
  [ USER_TASK_TIMEOUT_MS = <num> ]
  [ COPY GRANTS ]
  [ COMMENT = '<string_literal>' ]
  [ AFTER <string> ]
[ WHEN <boolean_expr> ]
AS
  <sql>;
                 
--CREATE DATABASE
CREATE OR REPLACE DATABASE BUILDING_SS;

--SWITCH CONTEXT
USE DATABASE BUILDING_SS;

--CREATE SCHEMAS
CREATE SCHEMA STG;
CREATE SCHEMA CDC;
CREATE SCHEMA TGT;

--CREATE SEQUENCE
CREATE OR REPLACE SEQUENCE SEQ_01
START = 1
INCREMENT = 1;

--CREATE STAGING TABLE
CREATE OR REPLACE TABLE STG.CUSTOMER
(C_CUSTKEY NUMBER(38,0),
 C_NAME VARCHAR(25),
 C_PHONE VARCHAR(15));

CREATE OR REPLACE TABLE TGT.CUSTOMER
(C_CUSTSK int default SEQ_01.nextval,
  C_CUSTKEY NUMBER(38,0),
 C_NAME VARCHAR(25),
 C_PHONE VARCHAR(15),
 DATE_UPDATED TIMESTAMP DEFAULT CURRENT_TIMESTAMP());

CREATE STAGE "BUILDING_SS"."PUBLIC".S3_STAGE URL = s3://building-solutions-with-snowflake' CREDENTIALS = (AWS_KEY_ID = '**********' AWS_SECRET_KEY = '****************************************');
                 
--GRANT PERMISSIONS ON STAGE
GRANT USAGE ON STAGE S3_STAGE TO SYSADMIN;

--SHOW STAGES
SHOW STAGES;

--UNLOAD DATA TO S3 EXTERNAL STAGE
COPY INTO @S3_STAGE/Customer 
FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."CUSTOMER"
HEADER=TRUE;

--COPY INTO TABLE
COPY INTO STG.CUSTOMER (C_CUSTKEY, C_NAME, C_PHONE) 
FROM (SELECT $1, $2, $5 FROM  @S3_STAGE/)
FILE_FORMAT=(TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1 COMPRESSION = 'GZIP');

--CONFIRM YOU HAVE 150K RECORDS IN THE STAGING TABLE
SELECT COUNT(*) 
FROM STG.CUSTOMER;

--SEED TABLE
INSERT INTO TGT.CUSTOMER (C_CUSTKEY, C_NAME, C_PHONE)
SELECT  C_CUSTKEY,
        C_NAME,
        C_PHONE
FROM STG.CUSTOMER;

--CREATE STREAM
CREATE OR REPLACE STREAM CDC.CUSTOMER
ON TABLE STG.CUSTOMER;
                 
--SHOW STREAMS
SHOW STREAMS;

--CHECK CHANGE TABLE FOR METADATA COLUMNS
SELECT * 
FROM CDC.CUSTOMER;

                 
--RUN AN UPDATE ON THE STAGING TABLE
UPDATE STG.CUSTOMER
SET C_PHONE = '999'
WHERE C_CUSTKEY = 105002;

--CHECK CHANGE TABLE
SELECT * FROM CDC.CUSTOMER; 

--CHECK IF THE STREAM CONTAINS DATA
SELECT SYSTEM$STREAM_HAS_DATA('CDC.CUSTOMER');
                 
CREATE OR REPLACE TASK CDC.MERGE_CUSTOMER
  WAREHOUSE = COMPUTE_WH --YOU MUST SPECIFY A WAREHOUSE TO USE
  SCHEDULE = '5 minute' 
WHEN
  SYSTEM$STREAM_HAS_DATA('CDC.CUSTOMER')
AS
MERGE INTO TGT.CUSTOMER TGT
USING CDC.CUSTOMER CDC
ON TGT.C_CUSTKEY = CDC.C_CUSTKEY
WHEN MATCHED AND METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = 'TRUE'
THEN UPDATE SET TGT.C_NAME = CDC.C_NAME, TGT.C_PHONE = CDC.C_PHONE
WHEN NOT MATCHED AND METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = 'FALSE' THEN
INSERT (C_CUSTKEY, C_NAME, C_PHONE) VALUES (C_CUSTKEY, C_NAME, C_PHONE)
WHEN MATCHED AND METADATA$ACTION = 'DELETE' AND METADATA$ISUPDATE = 'FALSE' THEN
DELETE;

--BY DEFAULT A TASK IS SET UP IN SUSPEND MODE
SHOW TASKS;

--ENSURE SYSADMIN CAN EXECUTE TASKS
USE ROLE accountadmin;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE SYSADMIN;

--YOU NEED TO RESUME THE TASK TO ENABLE IT
ALTER TASK CDC.MERGE_CUSTOMER RESUME;

--Use the TASK_HISTORY table function to monitor your task status. 
SHOW TASKS;
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME=>'MERGE_CUSTOMER'));
                 
--CHECK THE TARGET TABLE
SELECT *
FROM TGT.CUSTOMER
WHERE C_CUSTKEY = 105002;

--insert a new record into our staging table.
INSERT INTO STG.CUSTOMER (C_CUSTKEY, C_NAME, C_PHONE) 
SELECT 99999999, 'JOE BLOGGS', '1234-5678';
 
--confirm the change are processed into the target table.
SELECT * FROM TGT.CUSTOMER
WHERE C_CUSTKEY = 99999999;

--DELETE A RECORD FROM STAGING
DELETE FROM STG.CUSTOMER WHERE C_CUSTKEY = 99999999;
                 
--CLEAN UP
DROP DATABASE BUILDING_SS;
ALTER WAREHOUSE COMPUTE_WH SUSPEND;



                 