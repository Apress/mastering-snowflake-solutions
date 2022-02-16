//CREATE DATABASE
CREATE OR REPLACE DATABASE TIME_TRAVEL;

//CREATE A TABLE WITH THE DATA RETENTION PERIOD SET
CREATE TABLE DIM_CUSTOMER(CUSTOMER_SK INT, CUSTOMER_BK INT, CUSTOMER_FIRST_NAME VARCHAR(100))
DATA_RETENTION_TIME_IN_DAYS = 90;

//CHECK THE RETENTION TIME
SHOW TABLES;

//ALTER THE DATA RETENTION PERIOD
ALTER TABLE DIM_CUSTOMER SET DATA_RETENTION_TIME_IN_DAYS=30;

//SELECTING FROM A TABLE USING TIME TRAVEL WITH A TIMESTAMP
SELECT * 
FROM DIM_CUSTOMER AT(TIMESTAMP => '2021-06-07 02:21:10.00 -0700'::timestamp_tz);

//QUERYING TABLE DATA AT A 15 MINS AGO USING A TIME OFFSET
SELECT * 
FROM DIM_CUSTOMER AT(OFFSET => -60*15);

//QUERYING TABLE DATA, UP TO BUT NOT INCLUDING ANY CHANGES MADE BY THE SPECIFIED STATEMENT
SELECT * 
FROM DIM_CUSTOMER BEFORE(STATEMENT => '019db306-3200-7542-0000-00006bb5d821');

//YOU CAN LIST OUT ANY DROPPED OBJECTS USING THE SHOW COMMAND ALONG WITH THE HISTORY KEYWORD:
SHOW TABLES HISTORY LIKE '%DO_NOT_DROP'; 

SHOW SCHEMAS HISTORY IN SALES_DB;

SHOW DATABASES HISTORY;


//CREATE DATABASE
CREATE OR REPLACE DATABASE TIME_TRAVEL;

//CREATE A SEQUENCE TO USE FOR THE TABLE. 
//WE'LL USE THIS LATER WHEN WE PINPOINT A RECORD TO DELTE
CREATE OR REPLACE SEQUENCE SEQ_TIME_TRAVEL
START = 1
INCREMENT = 1;

//CREATE A TABLE
CREATE OR REPLACE TABLE DO_NOT_DROP
(ID NUMBER,
VERY VARCHAR(10),
IMPORTANT VARCHAR(20),
TABLE_DATA VARCHAR(10));
 
//INSERT 100 RECORDS INTO THE TABLE FROM THE SNOWFLAKE SAMPLE DATA
INSERT INTO DO_NOT_DROP 
SELECT SEQ_TIME_TRAVEL.NEXTVAL, 'VERY', 'IMPORTANT', 'DATA'
FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."CUSTOMER"
LIMIT 100;
 
//CONFIRM WE HAVE 100 RECORDS
SELECT COUNT(*) FROM DO_NOT_DROP;
 
//DROP THE TABLE - OOPS!
DROP TABLE DO_NOT_DROP;
 
//LOOK AT THE HISTORY OF THIS TABLE 
//NOTE THE VALUE IN THE DROPPED_ON COLUMN
SHOW TABLES HISTORY LIKE '%DO_NOT_DROP';
 
//UNDROP THE TABLE TO RESTORE IT
UNDROP TABLE DO_NOT_DROP;
 
//CONFIRM THE TABLE IS BACK WITH 100 RECORDS
SELECT COUNT(*) FROM DO_NOT_DROP;
 
//REVIEW THE TABLE METADATA AGAIN
SHOW TABLES HISTORY LIKE '%DO_NOT_DROP';
 
//IMPORTANT: WAIT A FEW MINUTES BEFORE RUNNING THE NEXT 
//BATCH OF QUERIES. THIS ALLOWS FOR A GOOD PERIOD OF TIME 
//TO QUERY THE TABLE BEFORE WE  
//DELETE A SINGLE RECORD FROM THE TABLE
DELETE FROM DO_NOT_DROP
WHERE ID = (SELECT MAX(ID)
             FROM DO_NOT_DROP);

//CHECK THE METADATA TO GET THE MOST RECENT QUERY_ID (RELATING TO THE QUERY ABOVE)
SET QUERY_ID = 
(SELECT TOP 1 QUERY_ID
FROM TABLE (INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_TEXT LIKE 'DELETE FROM DO_NOT_DROP%'
ORDER BY START_TIME DESC);

//CHECK THE VALUE STORED
SELECT $QUERY_ID;

//CREATE A CLONE OF THE ORIGINAL TABLE USING THE QUERY_ID OBTAINED
//FROM THE QUERY ABOVE
CREATE OR REPLACE TABLE DO_NOT_DROP_V2 CLONE DO_NOT_DROP
BEFORE (STATEMENT => $QUERY_ID);
  
//COMPARE BOTH TABLES TO VIEW THE 1 RECORD DIFFERENCE
SELECT * 
FROM DO_NOT_DROP_V2 V2
LEFT JOIN DO_NOT_DROP V1 ON V2.ID = V1.ID
WHERE V1.ID IS NULL;

//RUN THE QUERY USING AN OFFEST TO A FEW MINTUES EARLIER AGAINST 
//THE ORIGINAL TABLE. THIS QUERY WONT RETURN ANY RECORDS.
SELECT * 
FROM DO_NOT_DROP_V2 V2
LEFT JOIN DO_NOT_DROP AT(OFFSET => -60*7) V1 ON V2.ID = V1.ID
WHERE V1.ID IS NULL;

 