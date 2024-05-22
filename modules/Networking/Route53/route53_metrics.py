from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def route53_metrics(file_path, session, region, account, metrics, time_generated):
    next_token = None
    idx = 0
    client = session.client('route53', region_name=region)
    account_id = account['account_id']
    while True:
        try:
            inventory = []
            response = client.list_hosted_zones(
                Marker=next_token) if next_token else client.list_hosted_zones()
            for resource in response.get('HostedZones', []):
                inventory.append(resource['Id'].split('/')[-1])
            if inventory:
                metrics = get_resource_utilization_metric(
                    session, region, inventory, account, metrics, time_generated)
                save_as_file_parquet_metrics(metrics, file_path, generate_parquet_prefix(
                    str(stack()[0][3]), region, f'{account_id}-metrics', idx))
            next_token = response.get('NextMarker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
