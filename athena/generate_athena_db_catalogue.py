"""

"""
import time
import boto3
import pandas as pd
import io

ATHENA_QUERY_RESULT_BUCKET = 's3://aws-athena-query-results-531621816246-us-east-1/'
BUCKET_NAME = "my-datalake"
TARGET_LOC = "athena/table_schemas"

aws_session = boto3.Session(region_name="us-east-1")
client = boto3.client('athena', region_name="us-east-1")

def get_databases():
    get_databases = client.start_query_execution(
        QueryString=f"SHOW DATABASES",
        ResultConfiguration={
            'OutputLocation': ATHENA_QUERY_RESULT_BUCKET
        }
    )
    time.sleep(2)
    get_databases_result = client.get_query_results(QueryExecutionId=get_databases['QueryExecutionId'])
    tables = [row['Data'][0]['VarCharValue'] for row in get_databases_result['ResultSet']['Rows']]
    return tables

def get_tables(db_name):
    get_tables = client.start_query_execution(
        QueryString=f"SHOW TABLES IN {db_name}",
        ResultConfiguration={
            'OutputLocation': ATHENA_QUERY_RESULT_BUCKET
        }
    )
    time.sleep(2)
    get_tables_result = client.get_query_results(QueryExecutionId=get_tables['QueryExecutionId'])
    tables = [row['Data'][0]['VarCharValue'] for row in get_tables_result['ResultSet']['Rows']]
    return tables

def get_table_ddl(db_name,table_name):
    table_ddl = ''
    athena_query = client.start_query_execution(
        QueryString=f"SHOW CREATE TABLE {db_name}.{table_name}",
        QueryExecutionContext={
            'Database': db_name
        },
        ResultConfiguration={
            'OutputLocation': ATHENA_QUERY_RESULT_BUCKET
        }
    )
    print(f"Getting DDL for the table '{table_name}' ...")
    time.sleep(3)
    query_result = client.get_query_results(QueryExecutionId=athena_query['QueryExecutionId'])
    for row in query_result['ResultSet']['Rows']:
        table_ddl += (row['Data'][0]['VarCharValue']).replace('`', '')
    return table_ddl

def generate_db_catalogue(db_name):
    print(f'db_name = {db_name}')
    ddl_catalogue = []

    # Get the tables in the database
    tables = get_tables(db_name)
    print(f'No. tables = {len(tables)} : {tables}')

    # Get the ddls for each table
    for table in tables:
        ddl_catalogue.append(
            {
                "db_name": db_name,
                "table_name": table,
                "table_ddl": get_table_ddl(db_name=db_name, table_name=table)
            }
        )

    # Write dataframe to S3 in JSON
    client_s3 = boto3.client('s3', region_name="us-east-1")
    client_s3.put_object(
        Body = str(ddl_catalogue),
        Bucket = BUCKET_NAME,
        Key=f'{TARGET_LOC}/{db_name}.json'
    )

def main():
    db_list = get_databases()
    print(f"db_list = {db_list}")

    for db in db_list:
        generate_db_catalogue(db_name=db)

if __name__ == "__main__":
    main()