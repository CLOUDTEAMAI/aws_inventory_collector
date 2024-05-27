from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def msk_nodes_metrics(file_path, session, region, account, metrics, time_generated):
    next_token = None
    idx = 0
    client = session.client('kafka', region_name=region)
    account_id = account['account_id']
    while True:
        try:
            clusters_inventory = []
            response = client.list_clusters_v2(
                NextToken=next_token) if next_token else client.list_clusters_v2()
            for resource in response.get('ClusterInfoList', []):
                clusters_inventory.append(resource['ClusterArn'])
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
    if clusters_inventory:
        for cluster in clusters_inventory:
            while True:
                try:
                    cluster_name = cluster.split('/')[1]
                    gw_idx = 0
                    inventory = []
                    addons = {"type": "msk-nodes"}
                    addons['nodes'] = []
                    tgw_idx = {}
                    response = client.list_nodes(
                        NextToken=next_token, ClusterArn=cluster) if next_token else client.list_nodes(ClusterArn=cluster)
                    for resource in response.get('NodeInfoList', []):
                        resource_name = resource.get(
                            'BrokerNodeInfo', {}).get('BrokerId')
                        inventory.append(resource_name)
                        if cluster_name in tgw_idx:
                            addons['nodes'][tgw_idx[cluster_name]]['nodes'].append(
                                resource_name)
                        else:
                            tgw_idx[cluster_name] = gw_idx
                            addons['nodes'].append(
                                {"Cluster Name": cluster_name, "nodes": []})
                            addons['nodes'][tgw_idx[cluster_name]]['nodes'].append(
                                resource_name)
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
