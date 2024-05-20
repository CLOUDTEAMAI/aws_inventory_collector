from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_route53(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('route53', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_hosted_zones(
                Marker=next_token) if next_token else client.list_hosted_zones()
            for i in response.get('HostedZones', []):
                arn = f"arn:aws:route53::{':'.join(i['Id'].split('/'))}"
                inventory_object = extract_common_info(
                    arn, i, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path,
                                 generate_parquet_prefix(str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextMarker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
