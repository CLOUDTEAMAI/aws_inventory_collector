from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_appflow(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('appflow', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_flows(
                nextToken=next_token) if next_token else client.list_flows()
            for resource in response.get('flows', []):
                if 'createdAt' in resource:
                    resource['createdAt'] = resource['createdAt'].isoformat()
                if 'lastUpdatedAt' in resource:
                    resource['lastUpdatedAt'] = resource['lastUpdatedAt'].isoformat()
                if 'mostRecentExecutionTime' in resource['lastRunExecutionDetails']:
                    resource['lastRunExecutionDetails']['mostRecentExecutionTime'] = resource[
                        'lastRunExecutionDetails']['mostRecentExecutionTime'].isoformat()
                flow_name = resource['flowName']
                arn = f"arn:aws:appflow:{region}:{account_id}:flow/{flow_name}"
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
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
