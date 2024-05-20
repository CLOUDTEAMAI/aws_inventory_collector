from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_storagegateway(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('storagegateway', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_gateways(
                Marker=next_token) if next_token else client.list_gateways()
            for resource in response.get('Gateways', []):
                arn = resource['GatewayARN']
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('Marker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
