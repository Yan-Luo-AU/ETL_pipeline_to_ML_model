import json
import boto3
import time

athena_client = boto3.client('athena')


def lambda_handler(event, context):
    
    database = event['database']
    query_output = event['query_output']
    
    # TODO implement
    query1 = """
    drop table if exists user_features_1
    """
    
    query2 = """
    
    create table user_features_1 with (external_location = 's3://[my-s3-bucket]/features/user_features_1/', format = 'parquet')
    as (
    select user_id, 
       Max(order_number) as user_orders, 
       Sum(days_since_prior_order) as user_period, 
       Avg(days_since_prior_order) as user_mean_days_since_prior
    from order_products_prior
    group by user_id
  
  );
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
