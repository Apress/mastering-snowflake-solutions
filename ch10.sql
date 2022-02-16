//SNOWSQL CODE
CREATE OR REPLACE database development;

SELECT current_database();

CREATE OR REPLACE TABLE CUST_ORDERS (
  order_id int,
  first_name string,
  last_name string,
  email string,
  street string,
  city string
  );

CREATE OR REPLACE WAREHOUSE development_wh with
  warehouse_size='X-SMALL'
  auto_suspend = 180
  auto_resume = true
  initially_suspended=true;

put file://c:\temp\orders*.csv @development.public.%cust_orders;

list @development.public.%cust_orders;

COPY INTO cust_orders
  from @development.public.%cust_orders
  file_format = (type = csv)
  on_error = 'skip_file';

SELECT * FROM cust_orders where first_name = = 'Ron';;

INSERT INTO CUST_ORDERS VALUES
  (999,'Clementine','Adamou','cadamou@development.com','10510 Sachs Road','Klenak') ,
  (9999,'Marlowe','De Anesy','madamouc@development.co.uk','36768 Northfield Plaza','Fangshan');

DROP DATABASE IF EXISTS DEVELOPMENT;

!EXIT

