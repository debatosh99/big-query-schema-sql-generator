
  CREATE EXTERNAL TABLE `trim-strength-477307-h0.customer.customer_details_external_no_blms`
  WITH CONNECTION `trim-strength-477307-h0.us-central1.learnbiglakeiceberg3_bq_connection`
  OPTIONS (
         format = 'ICEBERG',
         uris = ["gs://learnbiglakeiceberg3/metadata/v1762638041.metadata.json"]
   );

select * from `trim-strength-477307-h0.customer.customer_details_external_no_blms`;

Note:  Replace the uris value with the latest JSON metadata file for a specific table snapshot.