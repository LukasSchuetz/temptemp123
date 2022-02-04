import json
import os
import re
import uuid

import boto3
import pandas as pd

S3_BUCKET_NAME = 'bmw-e2e-dev-google-trends'

DAILY_FILENAME_PATTERN = r'search_volume\/daily_\d{4}_\d{2}_\d{2}_.*\.snappy\.parquet'
# Example: daily_2022_01_24_69de6ba8-cfd5-4adc-8b5c-48c296ba9cb5.snappy.parquet
MONTHLY_FILENAME_PATTERN = r'search_volume\/monthly_\d{4}_\d{2}_.*\.snappy\.parquet'
# Example: monthly_2022_01_c4a3d3e8-d948-4672-b043-58c2c68c8c32.snappy.parquet
CURRENT_FILENAME_PATTERN = r'search_volume\/\d{4}\d{2}\d{2}_.*\.snappy\.parquet'


def lambda_handler(event, context):
    """
    This function checks the S3 bucket for parquet files with daily data and combines them in another parquet file with monthly data
    Naming conventions in the filenames are used to differentiate between daily and monthly data
    """

    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    bucket_files = s3_client.list_objects(Bucket=S3_BUCKET_NAME)

    # Only get relevant paths that start with /search_volume/
    search_volume_files = [file['Key'] for file in bucket_files['Contents'] if
                           re.match(r"search_volume\/.+", file['Key'])]

    # Only get historical quentin files
    quentin_files = [filepath for filepath in search_volume_files if not re.match(CURRENT_FILENAME_PATTERN, filepath)]
    amount = len(quentin_files)
    print(f'Found {amount} quentin files')

    # Combine all found files to one large dataset
    dataframes_to_concatenate = []
    for filepath in quentin_files:
        filename = filepath.split('/').pop()
        s3_resource.Object(S3_BUCKET_NAME, filepath).download_file(f'/tmp/{filename}')
        df = pd.read_parquet(f'/tmp/{filename}', engine='fastparquet')
        dataframes_to_concatenate.append(df)
    print(1)
    concatenated_df = pd.concat(dataframes_to_concatenate)
    print(2)
    concatenated_df['year_month'] = concatenated_df.apply(
        lambda row: str(row['date'].year) + '_' + str(row['date'].month), axis=1)
    print(3)
    temp_folder_path = '/tmp/partitioned_files'
    concatenated_df.to_parquet(temp_folder_path, partition_cols=['year_month'], times='int96')

    for directory in os.scandir(temp_folder_path):
        if directory.is_dir():
            year, month = directory.name.split('=')[1].split('_')
            month = month.zfill(2)
            unique_id = str(uuid.uuid4())
            filename = f'monthly_{year}_{month}_{unique_id}.snappy.parquet'
            print(f'4 in dir {filename}')
            print(filename)
            s3_resource.Object('bmw-e2e-test-bucket', f'search_volume/{filename}').put(
                Body=open(f'{temp_folder_path}/{directory.name}/part.0.parquet', 'rb'))

    return {
        'statusCode': 200,
        'body': json.dumps('Success')
    }

if __name__ == "__main__":
    lambda_handler(None, None)