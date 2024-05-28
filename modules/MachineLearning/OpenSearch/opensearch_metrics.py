from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def opensearch_metrics(file_path, session, region, account, metrics, time_generated):
    next_token = None
    idx = 0
    client = session.client('opensearch', region_name=region)
    account_id = account['account_id']
    response = client.list_domain_names()
    names = []
    for resource in response.get('DomainNames', []):
        names.append(resource['DomainName'])
    while True:
        try:
            gw_idx = 0
            inventory = []
            addons = {"type": "opensearch"}
            addons['nodes'] = []
            tgw_idx = {}
            response = client.describe_domains(
                DomainNames=names)
            for resource in response.get('DomainStatusList', []):
                nodes_response = client.describe_domain_nodes(
                    DomainName=resource['DomainName'])
                inventory.append(resource['DomainName'])
                for node in nodes_response.get('DomainNodesStatusList', []):
                    if resource['DomainName'] in tgw_idx:
                        addons['nodes'][tgw_idx[resource['DomainName']]]['nodes'].append(
                            node.get('NodeId', ''))
                        if not addons['nodes'][tgw_idx[resource['DomainName']]].get('items', {}):
                            addons['nodes'][tgw_idx[resource['DomainName']]
                                            ]['items'] = {}
                        addons['nodes'][tgw_idx[resource['DomainName']]]['items'][node.get('NodeId', '')] = {
                            "NodeId": node.get('NodeId', ''),
                            "ClientId": account_id
                        }
                    else:
                        tgw_idx[resource['DomainName']] = gw_idx
                        addons['nodes'].append(
                            {"DomainName": resource['DomainName'], "nodes": []})
                        addons['nodes'][tgw_idx[resource['DomainName']]]['nodes'].append(
                            node.get('NodeId', ''))
                        if not addons['nodes'][tgw_idx[resource['DomainName']]].get('items', {}):
                            addons['nodes'][tgw_idx[resource['DomainName']]
                                            ]['items'] = {}
                        addons['nodes'][tgw_idx[resource['DomainName']]]['items'][node.get('NodeId', '')] = {
                            "NodeId": node.get('NodeId', ''),
                            "ClientId": account_id
                        }
                gw_idx = gw_idx + 1
            if inventory:
                metrics = get_resource_utilization_metric(
                    session, region, inventory, account, metrics, time_generated, addons)
                save_as_file_parquet_metrics(metrics, file_path, generate_parquet_prefix(
                    str(stack()[0][3]), region, f'{account_id}-metrics', idx))
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
