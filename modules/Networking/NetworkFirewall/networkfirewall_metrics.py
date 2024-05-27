from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric, list_az


def networkfirewall_metrics(file_path, session, region, account, metrics, time_generated):
    next_token = None
    idx = 0
    client = session.client('network-firewall', region_name=region)
    account_id = account['account_id']
    while True:
        try:
            gw_idx = 0
            inventory = []
            addons = {"type": "networkfirewall"}
            addons['nodes'] = []
            tgw_idx = {}
            response = client.list_firewalls(
                NextToken=next_token) if next_token else client.list_firewalls()
            for resource in response.get('Firewalls', []):
                inventory.append(resource['FirewallName'])
                for az in list_az(session, region):
                    if resource['FirewallName'] in tgw_idx:
                        addons['nodes'][tgw_idx[resource['FirewallName']]]['nodes'].append(
                            az)
                    else:
                        tgw_idx[resource['FirewallName']] = gw_idx
                        addons['nodes'].append(
                            {"FirewallName": resource['FirewallName'], "nodes": []})
                        addons['nodes'][tgw_idx[resource['FirewallName']]]['nodes'].append(
                            az)
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
