import os
import boto3

def upload_file_to_S3(input_path, s3_path):
    """
     Upload only CSV files to S3 Bucket
    Args:
        input_path: CSV file path to upload S3.
        s3_path: S3 path to upload CSV file.

    Returns:
        File upload sucessfully return True otherwise False.

    """
    try:
        if input_path.endswith('.csv') == False:
            raise (Exception("Not CSV File"))

        elif os.stat(input_path).st_size == 0:
            raise (Exception("File Empty"))

        else:
            bucket = s3_path.replace('s3://', '').split('/')[0]
            print(f'bucket = {bucket}')

            s3_obj_key = s3_path.replace(f's3://{bucket}/', '')
            print(f's3_obj_key = {s3_obj_key}')

            file_name = input_path.split('/')[-1]
            print(f'file_name = {file_name}')

            s3client = boto3.client('s3')
            s3client.upload_file(input_path, bucket, f'{s3_obj_key}/{file_name}')

    except Exception as e:
        print('Fail to upload')
        return False
    else:
        return True