import json
import boto3
import time

athena_client = boto3.client('athena')


def lambda_handler(event, context):
    
    database = event['database']
    query_output = event['query_output']
    
    # TODO implement
    query1 = """
    drop table if exists prod.up_features
    """
    
    query2 = """
    
    create table up_features with (external_location = 's3://[my-s3-bucket]/features/up_features/', format = 'parquet')
     as (
      select
  user_id,
  product_id,
  sum(order_number) as total_number_of_orders, 
  min(order_number) as minimum_order_number,
  max(order_number) as maximum_order_number, 
  avg(add_to_cart_order) as average_add_to_cart_order 
from order_products_prior
group by user_id, product_id
       )
       ;

   """
    response1 = athena_client.start_query_execution (
        QueryString = query1,
        QueryExecutionContext = {
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': query_output
        }
        )
        
            #sleep 10 seconds to make sure the table is successfully dropped
    time.sleep(10)
        
    response2 = athena_client.start_query_execution(
            QueryString=query2,
            QueryExecutionContext={
                'Database':database
            },
            ResultConfiguration={
                'OutputLocation': query_output
            }
            )
            
        #get the query execution id
        
    execution_id = response2['QueryExecutionId']
        
    while True:
        stats = athena_client.get_query_execution(QueryExecutionId=execution_id)
        status = stats['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
    time.sleep(0.2) #200ms
    return {
        'statusCode': status
    }

