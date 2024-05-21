from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def elasticache_metrics(file_path, session, region, account, metrics, time_generated):
    next_token = None
    idx = 0
    client = session.client('elasticache', region_name=region)
    account_id = account['account_id']
    account_name = str(account.get('account_name', '')).replace(" ", "_")
    while True:
        try:
            clusters_idx = 0
            inventory = []
            addons = {"type": "elasticache"}
            addons['nodes'] = []
            response = client.describe_cache_clusters(
                Marker=next_token) if next_token else client.describe_cache_clusters()
            for resource in response.get('CacheClusters', []):
                inventory.append(resource['CacheClusterId'])
                addons['nodes'].append(
                    {"CacheClusterId": resource['CacheClusterId'], "nodes": []})
                if resource.get('NumCacheNodes', 1) <= 1:
                    addons['nodes'][clusters_idx]['nodes'].append('0001')
                else:
                    for node in resource.get('CacheNodes', []):
                        addons['nodes'][clusters_idx]['nodes'].append(
                            node.get('CacheNodeId', ''))
                clusters_idx = clusters_idx + 1
            if inventory:
                metrics = get_resource_utilization_metric(
                    session, region, inventory, account, metrics, time_generated, addons)
                save_as_file_parquet_metrics(metrics, file_path, generate_parquet_prefix(
                    str(stack()[0][3]), region, f'{account_id}-metrics', idx))
            next_token = response.get('Marker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
