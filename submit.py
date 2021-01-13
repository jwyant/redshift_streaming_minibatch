#!/usr/bin/env python
import os
import boto3
from urllib.parse import urlparse
import pg8000

# Get statement from S3
sql_s3_uri = os.environ['Redshift_Sql']
parsed_s3 = urlparse(sql_s3_uri)
sql_bucket = parsed_s3.netloc
sql_key = parsed_s3.path.lstrip('/')
s3 = boto3.resource('s3')
obj = s3.Object(sql_bucket, sql_key)
statement = obj.get()['Body'].read().decode('utf-8') 

# Get rest of environment variables
cluster_id = os.environ['Redshift_ClusterIdentifier']
database = os.environ['Redshift_Database']
secret_arn = os.environ['Redshift_SecretArn']
statement_name = os.environ['Redshift_StatementName']


conn = pg8000.connect()
# redshift_data = boto3.client('redshift-data')

# response = redshift_data.execute_statement(
#     ClusterIdentifier=cluster_id,
#     Database=database,
#     SecretArn=secret_arn,
#     Sql=statement,
#     StatementName=statement_name,
#     WithEvent=False
# )

print(response['Id'])
