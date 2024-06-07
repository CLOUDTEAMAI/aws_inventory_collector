from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def vpn_metrics(file_path, session, region, account, metrics, time_generated):
    next_token = None
    idx = 0
    client = session.client('ec2', region_name=region)
    account_id = account['account_id']
    while True:
        try:
            inventory = []
            response = client.describe_vpn_connections(
                NextToken=next_token) if next_token else client.describe_vpn_connections()
            for resource in response.get('VpnConnections', []):
                for tunnel in resource['Options']['TunnelOptions']:
                    inventory.append(tunnel['OutsideIpAddress'])
            if inventory:
                metrics = get_resource_utilization_metric(
                    session, region, inventory, account, metrics, time_generated)
                save_as_file_parquet_metrics(metrics, file_path, generate_parquet_prefix(
                    str(stack()[0][3]), region, f'{account_id}-metrics', idx))
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def clientvpn_metrics(file_path, session, region, account, metrics, time_generated):
    next_token = None
    idx = 0
    client = session.client('ec2', region_name=region)
    account_id = account['account_id']
    while True:
        try:
            inventory = []
            response = client.describe_client_vpn_endpoints(
                NextToken=next_token) if next_token else client.describe_client_vpn_endpoints()
            for resource in response.get('ClientVpnEndpoints', []):
                inventory.append(resource['ClientVpnEndpointId'])
            if inventory:
                metrics = get_resource_utilization_metric(
                    session, region, inventory, account, metrics, time_generated)
                save_as_file_parquet_metrics(metrics, file_path, generate_parquet_prefix(
                    str(stack()[0][3]), region, f'{account_id}-metrics', idx))
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
