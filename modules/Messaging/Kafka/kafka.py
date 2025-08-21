from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_msk(file_path, session, region, time_generated, account, boto_config):
    next_token = None
    idx = 0
    client = session.client('kafka', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_clusters_v2(
                NextToken=next_token) if next_token else client.list_clusters_v2()
            for resource in response.get('ClusterInfoList', []):
                if 'CreationTime' in resource:
                    resource['CreationTime'] = resource['CreationTime'].isoformat()
                arn = resource['ClusterArn']
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def list_msk_nodes(file_path, session, region, time_generated, account, boto_config):
    next_token = None
    idx = 0
    client = session.client('kafka', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            clusters_inventory = []
            response = client.list_clusters_v2(
                NextToken=next_token) if next_token else client.list_clusters_v2()
            for resource in response.get('ClusterInfoList', []):
                clusters_inventory.append(resource['ClusterArn'])
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
    if clusters_inventory:
        for cluster in clusters_inventory:
            while True:
                try:
                    inventory = []
                    response = client.list_nodes(
                        NextToken=next_token, ClusterArn=cluster) if next_token else client.list_nodes(ClusterArn=cluster)
                    for resource in response.get('NodeInfoList', []):
                        arn = resource['NodeARN']
                        inventory_object = extract_common_info(
                            arn, resource, region, account_id, time_generated, account_name)
                        inventory.append(inventory_object)
                    save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                        str(stack()[0][3]), region, account_id, idx))
                    next_token = response.get('NextToken', None)
                    idx = idx + 1
                    if not next_token:
                        break
                except Exception as e:
                    print(e)
                    break
