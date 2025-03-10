from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def s3_metrics(file_path, session, region, account, metrics, time_generated, boto_config):
    next_token = None
    idx = 0
    client = session.client('s3', region_name=region, config=boto_config)
    account_id = account['account_id']
    while True:
        try:
            inventory = []
            response = client.list_buckets()
            for resource in response.get('Buckets', []):
                inventory.append(resource['Name'])
            if inventory:
                metrics = get_resource_utilization_metric(
                    session, region, inventory, account, metrics, time_generated)
                save_as_file_parquet_metrics(metrics, file_path, generate_parquet_prefix(
                    str(stack()[0][3]), region, f'{account_id}-metrics', idx))
            next_token = response.get('ContinuationToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
