from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_datasync_agents(file_path, session, region, time_generated, account, boto_config):
    next_token = None
    idx = 0
    client = session.client('datasync', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_agents(
                NextToken=next_token) if next_token else client.list_agents()
            for resource in response.get('Agents', []):
                arn = resource.get('AgentArn')
                agent_response = client.describe_agent(
                    AgentArn=arn)
                if 'CreationTime' in agent_response:
                    agent_response['CreationTime'] = agent_response['CreationTime'].isoformat(
                    )
                if 'LastConnectionTime' in agent_response:
                    agent_response['LastConnectionTime'] = agent_response['LastConnectionTime'].isoformat(
                    )
                inventory_object = extract_common_info(
                    arn, agent_response, region, account_id, time_generated, account_name)
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
