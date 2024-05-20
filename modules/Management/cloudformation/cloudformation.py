from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_cloudformation(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('cloudformation', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_stacks(
                NextToken=next_token) if next_token else client.describe_stacks()
            for resource in response.get('Stacks', []):
                if 'CreationTime' in resource:
                    resource['CreationTime'] = resource['CreationTime'].isoformat()
                if 'DeletionTime' in resource:
                    resource['DeletionTime'] = resource['DeletionTime'].isoformat()
                if 'LastUpdatedTime' in resource:
                    resource['LastUpdatedTime'] = resource['LastUpdatedTime'].isoformat()
                if 'LastCheckTimestamp' in resource['DriftInformation']:
                    resource['DriftInformation']['LastCheckTimestamp'] = resource['DriftInformation']['LastCheckTimestamp'].isoformat()

                arn = resource['StackId']
                resouce_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(resouce_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
