from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def vpcendpoint_metrics(file_path, session, region, account, metrics, time_generated):
    next_token = None
    idx = 0
    account_id = account['account_id']
    while True:
        try:
            vpe_idx = 0
            client = session.client('ec2', region_name=region)
            inventory = []
            addons = {"type": "privatelinkendpoints"}
            addons['nodes'] = []
            vpes_idx = {}
            response = client.describe_vpc_endpoints(
                NextToken=next_token) if next_token else client.describe_vpc_endpoints()
            for resource in response.get('VpcEndpoints', []):
                inventory.append(resource['VpcId'])
                if resource['VpcId'] in vpes_idx:
                    addons['nodes'][vpes_idx[resource['VpcId']]]['nodes'].append(
                        resource.get('VpcEndpointId', ''))
                    if not addons['nodes'][vpes_idx[resource['VpcId']]].get('items', []):
                        addons['nodes'][vpes_idx[resource['VpcId']]]['items'] = {}
                    addons['nodes'][vpes_idx[resource['VpcId']]]['items'][resource.get('VpcEndpointId', '')] = {
                        "Service Name": resource.get('ServiceName', ''),
                        "Endpoint Type": resource.get('VpcEndpointType', 'Interface'),
                        'Vpc Id': resource['VpcId']
                    }
                else:
                    vpes_idx[resource['VpcId']] = vpe_idx
                    addons['nodes'].append(
                        {"VpcId": resource['VpcId'], "nodes": []})
                    addons['nodes'][vpes_idx[resource['VpcId']]]['nodes'].append(
                        resource.get('VpcEndpointId', ''))
                    if not addons['nodes'][vpes_idx[resource['VpcId']]].get('items', []):
                        addons['nodes'][vpes_idx[resource['VpcId']]]['items'] = {}
                    addons['nodes'][vpes_idx[resource['VpcId']]]['items'][resource.get('VpcEndpointId', '')] = {
                        "Service Name": resource.get('ServiceName', ''),
                        "Endpoint Type": resource.get('VpcEndpointType', 'Interface'),
                        'VPC Id': resource['VpcId']
                    }
                    vpe_idx = vpe_idx + 1
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
