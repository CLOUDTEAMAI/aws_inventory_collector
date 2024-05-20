from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_s3_buckets(file_path, session, region='us-east-1', time_generated=None, account=None):
    next_token = None
    idx = 0
    client = session.client('s3', region_name=region)
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
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
