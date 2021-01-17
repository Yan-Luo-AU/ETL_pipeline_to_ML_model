import json
import boto3
import time

athena_client = boto3.client('athena')


def lambda_handler(event, context):
    
    database = event['database']
    query_output = event['query_output']
    
    # TODO implement
    query1 = """
    DROP TABLE IF EXISTS prod.order_products_prior
    """
    
    query2 = """
    create table order_products_prior with (external_location = 's3://[my-s3-bucket]/features/order_products_prior/', format='parquet')
    as (Select a.*,
    b.product_id,
    b.add_to_cart_order,
    b.reordered
FROM orders a
    Join order_products b
    On a.order_id = b.order_id
WHERE a.eval_set='prior');
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
