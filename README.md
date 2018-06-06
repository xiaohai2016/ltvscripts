### Reference file
https://docs.google.com/document/d/1PtCmyMg9f60u62DLpmWQhshB6LZIrMqwSRJOvzw7yZw/

### Download the scripts
git clone https://github.com/xiaohai2016/ltvscripts

### Entrypoint script files
* recreate_ltv_tables.py - to recreate LTV curated tables in mySQL (ltv_rider & ltv_driver).
* query_ltv_tables.py - to query the curated table in mySQL. It is in essence one call of fdp_http_api.queryData.
* API file - lib/fdp_python_http_api.py. One can this file to understand how the HTTP RESTful endpoint is invoked.

### How to recreate LTV curated tables
* Depending on the scope of query changes, one might need to recreate the LTV curated tables.
* To recreate the table, one needs to:
  * Paste your queries into the file ltv_driver_query.presto or ltv_rider_query.presto
  * Modify the mySQL table structure in file: ltv_driver_table_create.sql & ltv_rider_table_create.sql
  * Uncomment the corresponding line for table recreation in recreate_ltv_tables.py
  * Run the script file recreate_ltv_tables.py
  * You might encounter timeout (for now), ignore it. After a while, the data should be there

### How to query LTV curated tables
* You can test by directly running query_ltv_tables.py.
* You invoke the endpoint represented by "fdp_http_api.queryData" anyway you'd like (e.g. curl). Refer to the invocation of fdp_http_api.queryData in query_ltv_tables.py.
