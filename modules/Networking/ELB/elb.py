from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_elb(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('elb', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_load_balancers(
                Marker=next_token) if next_token else client.describe_load_balancers()
            for response in response.get('LoadBalancerDescriptions', []):
                if 'CreatedTime' in response:
                    response['CreatedTime'] = response['CreatedTime'].isoformat()
                arn = response['LoadBalancerArn']
                inventory_object = extract_common_info(
                    arn, response, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextMarker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def list_elbv2(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('elbv2', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_load_balancers(
                Marker=next_token) if next_token else client.describe_load_balancers()
            for resource in response.get('LoadBalancers', []):
                if 'CreatedTime' in resource:
                    resource['CreatedTime'] = resource['CreatedTime'].isoformat()
                arn = resource['LoadBalancerArn']
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextMarker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
