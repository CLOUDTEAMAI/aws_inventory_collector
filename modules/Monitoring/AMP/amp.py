from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_amp(file_path, session, region, time_generated, account, boto_config):
    next_token = None
    idx = 0
    client = session.client('amp', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_workspaces(
                nextToken=next_token) if next_token else client.list_workspaces()
            for resource in response.get('workspaces', []):
                arn = resource['workspaceArn']
                resource_desc = client.get_queue_attributes(
                    workspaceId=resource['workspaceId'])
                inventory_object = extract_common_info(
                    arn, resource_desc, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('nextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
