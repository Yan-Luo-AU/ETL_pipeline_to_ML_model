# Project IMBA - using AWS cloud services to construct ETL pipeline and ML model deployment
1. Raw Data in csv format saved in AWS S3
    * aisles.csv: 134 rows × 2
      * aisle_id(int), aisle(chr)
    * departments.csv: 21 rows × 2
      * department_id(int), department(chr)
    * order_products.csv: 33,819,106 rows × 4
      * order_id(int), product_id(int), add_to_cart_order(int), reorder(int)
    * orders.csv: 3,421,083 rows × 7
      * order_id(int), user_id(int), eval_set(chr), order_number(int), order_dow(int), order_hour_of_day(int), days_since_prior_order(num)
    * products.csv: 49,688 rows × 4
      * product_id(int), product_name(chr), aisle_id, department_id
