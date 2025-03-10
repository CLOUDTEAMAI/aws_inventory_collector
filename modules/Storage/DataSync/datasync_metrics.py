from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def datasync_agents_metrics(file_path, session, region, account, metrics, time_generated, boto_config):
    next_token = None
    idx = 0
    client = session.client('datasync', region_name=region, config=boto_config)
    account_id = account['account_id']
    while True:
        try:
            inventory = []
            response = client.list_agents(
                NextToken=next_token) if next_token else client.list_agents()
            for resource in response.get('Agents', []):
                inventory.append(resource['AgentArn'].split('/')[-1])
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
