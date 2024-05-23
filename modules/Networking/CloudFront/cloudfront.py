from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_cloudfront(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('cloudfront', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_distributions(
                Marker=next_token) if next_token else client.list_distributions()
            for resource in response['DistributionList'].get('Items', []):
                arn = resource.get('ARN', '')
                if 'LastModifiedTime' in resource:
                    resource['LastModifiedTime'] = resource['LastModifiedTime'].isoformat()
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
