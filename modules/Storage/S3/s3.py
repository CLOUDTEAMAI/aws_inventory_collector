import os
from botocore import exceptions
from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


class S3Manager:
    def __init__(self, session, bucket_name):
        self.bucket_name = bucket_name
        self.s3_client = session.client('s3')

    def list_files(self, prefix=''):
        """
        List files in the specified S3 bucket with an optional prefix.
        """
        try:
            files = []
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)

            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        files.append(obj['Key'])

            return files

        except exceptions.NoCredentialsError:
            print("Error: No AWS credentials found.")
        except exceptions.PartialCredentialsError:
            print("Error: Incomplete AWS credentials.")
        except exceptions.BotoCoreError as e:
            print(f"BotoCoreError: {e}")
        except exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'AccessDenied':
                print("Error: Access denied. Check your permissions.")
            else:
                print(f"ClientError: {e}")

    def download_file(self, s3_key, local_path):
        """
        Download a file from the specified S3 bucket.
        """
        try:
            print(f"Downloading {s3_key} to {local_path}")
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            print(f"Downloaded {s3_key} to {local_path}")
        except exceptions.NoCredentialsError:
            print("Error: No AWS credentials found.")
        except exceptions.PartialCredentialsError:
            print("Error: Incomplete AWS credentials.")
        except exceptions.BotoCoreError as e:
            print(f"BotoCoreError: {e}")
        except exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'AccessDenied':
                print("Error: Access denied. Check your permissions.")
            elif e.response['Error']['Code'] == 'NoSuchKey':
                print(f"Error: The object {s3_key} does not exist.")
            else:
                print(f"ClientError: {e}")


def list_s3_buckets(file_path, session, region='us-east-1', time_generated=None, account=None):
    next_token = None
    idx = 0
    client = session.client('s3', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_buckets()
            for resource in response.get('Buckets', []):
                resource['CreationDate'] = resource['CreationDate'].isoformat()
                arn = f"arn:aws:s3:::{resource['Name']}"
                try:
                    bucket_location = client.get_bucket_location(
                        Bucket=resource['Name'])['LocationConstraint']
                    inventory_object = extract_common_info(
                        arn, resource, bucket_location, account_id, time_generated, account_name)
                    inventory.append(inventory_object)
                except Exception as ex:
                    print(ex)
                    s3_object = extract_common_info(
                        arn, resource, None, account_id, time_generated, account_name)
                    inventory.append(s3_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), 'global', account_id, idx))
            next_token = response.get('ContinuationToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
