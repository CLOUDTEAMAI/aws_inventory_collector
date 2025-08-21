from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def cloudhsmv2_metrics(file_path, session, region, account, metrics, time_generated, boto_config):
    next_token = None
    idx = 0
    client = session.client(
        'cloudhsmv2', region_name=region, config=boto_config)
    account_id = account['account_id']
    while True:
        try:
            clusters_idx = 0
            inventory = []
            addons = {"type": "cloudhsmv2"}
            addons['nodes'] = []
            hsm_idx = {}
            response = client.describe_clusters(
                NextToken=next_token) if next_token else client.describe_clusters()
            for resource in response.get('Clusters', []):
                inventory.append(resource['ClusterId'])
                for hsm in resource.get('Hsms', []):
                    if resource['ClusterId'] in hsm_idx:
                        addons['nodes'][hsm_idx[resource['ClusterId']]]['nodes'].append(
                            hsm.get('HsmId', ''))
                        if not addons['nodes'][hsm_idx[resource['ClusterId']]].get('items', []):
                            addons['nodes'][hsm_idx[resource['ClusterId']]
                                            ]['items'] = {}
                        addons['nodes'][hsm_idx[resource['ClusterId']]]['items'][hsm.get('HsmId', '')] = {
                            'ClusterId': resource['ClusterId']
                        }
                    else:
                        hsm_idx[resource['ClusterId']] = clusters_idx
                        addons['nodes'].append(
                            {"ClusterId": resource['ClusterId'], "nodes": []})
                        addons['nodes'][hsm_idx[resource['ClusterId']]]['nodes'].append(
                            hsm.get('HsmId', ''))
                        if not addons['nodes'][hsm_idx[resource['ClusterId']]].get('items', []):
                            addons['nodes'][hsm_idx[resource['ClusterId']]
                                            ]['items'] = {}
                        addons['nodes'][hsm_idx[resource['ClusterId']]]['items'][hsm.get('HsmId', '')] = {
                            'ClusterId': resource['ClusterId']
                        }
                clusters_idx = clusters_idx + 1
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
