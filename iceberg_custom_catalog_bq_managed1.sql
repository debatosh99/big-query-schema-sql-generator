
DROP TABLE `trim-strength-477307-h0.customer.customer_details_iceberg_bq_managed`;


CREATE TABLE `trim-strength-477307-h0.customer.customer_details_iceberg_bq_managed`(
id INT64,name STRING,age INT64
)
WITH CONNECTION `trim-strength-477307-h0.us-central1.learnbiglakeiceberg3_bq_connection`
OPTIONS (
file_format = 'PARQUET',
table_format = 'ICEBERG',
storage_uri = 'gs://learnbiglakeiceberg3');


INSERT INTO `trim-strength-477307-h0.customer.customer_details_iceberg_bq_managed`(id, name, age) VALUES (100, 'Tom', 20),(200, 'Rom', 21),(300, 'Som', 22);


select * from `trim-strength-477307-h0.customer.customer_details_iceberg_bq_managed`;

EXPORT TABLE METADATA FROM `trim-strength-477307-h0.customer.customer_details_iceberg_bq_managed`
