import json
import boto3
from urllib.parse import urlparse

def lambda_handler(event, context):

    # Get rest of environment variables
    cluster_id = event['Redshift_ClusterIdentifier']
    database = event['Redshift_Database']
    secret_arn = event['Redshift_SecretArn']
    statement_name = event['Redshift_StatementName']
    sql = event['Redshift_Sql']

    redshift_data = boto3.client('redshift-data')

    response = redshift_data.execute_statement(
        ClusterIdentifier=cluster_id,
        Database=database,
        SecretArn=secret_arn,
        Sql=sql,
        StatementName=statement_name,
        WithEvent=True
    )

    return {
        'statusCode': 200,
        'body': {
            'Statement_Id': response['Id'],
        }
    }
