from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def elb_metrics(file_path, session, region, account, metrics, time_generated):
    next_token = None
    idx = 0
    client = session.client('elb', region_name=region)
    account_id = account['account_id']
    while True:
        try:
            inventory = []
            response = client.describe_load_balancers(
                Marker=next_token) if next_token else client.describe_load_balancers()
            for resource in response.get('LoadBalancerDescriptions', []):
                inventory.append(resource['LoadBalancerName'])
            if inventory:
                metrics = get_resource_utilization_metric(
                    session, region, inventory, account, metrics, time_generated)
                save_as_file_parquet_metrics(metrics, file_path, generate_parquet_prefix(
                    str(stack()[0][3]), region, f'{account_id}-metrics', idx))
            next_token = response.get('NextMarker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def elbv2_network_metrics(file_path, session, region, account, metrics, time_generated):
    next_token = None
    idx = 0
    client = session.client('elbv2', region_name=region)
    account_id = account['account_id']
    while True:
        try:
            inventory = []
            response = client.describe_load_balancers(
                Marker=next_token) if next_token else client.describe_load_balancers()
            for resource in response.get('LoadBalancers', []):
                if resource.get('Type', '').lower() == 'network':
                    inventory.append(resource.get(
                        'LoadBalancerArn').split(":loadbalancer/")[-1])
            if inventory:
                metrics = get_resource_utilization_metric(
                    session, region, inventory, account, metrics, time_generated)
                save_as_file_parquet_metrics(metrics, file_path, generate_parquet_prefix(
                    str(stack()[0][3]), region, f'{account_id}-metrics', idx))
            next_token = response.get('NextMarker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def elbv2_application_metrics(file_path, session, region, account, metrics, time_generated):
    next_token = None
    idx = 0
    account_id = account['account_id']
    while True:
        try:
            gw_idx = 0
            client = session.client('elbv2', region_name=region)
            inventory = []
            addons = {"type": "elbv2-application"}
            addons['nodes'] = []
            tgw_idx = {}
            response = client.describe_target_groups(
                Marker=next_token) if next_token else client.describe_target_groups()
            for resource in response.get('TargetGroups', []):
                resource_name = resource['TargetGroupArn'].split(':')[-1]
                inventory.append(resource_name)
                for lb in resource.get('LoadBalancerArns', []):
                    lb_resource_name = lb.split(":loadbalancer/")[-1]
                    if resource_name in tgw_idx:
                        addons['nodes'][tgw_idx[resource_name]]['nodes'].append(
                            lb_resource_name)
                    else:
                        tgw_idx[resource_name] = gw_idx
                        addons['nodes'].append(
                            {"TargetGroupName": resource_name, "nodes": []})
                        addons['nodes'][tgw_idx[resource_name]]['nodes'].append(
                            lb_resource_name)
                        gw_idx = gw_idx + 1
            if inventory:
                metrics = get_resource_utilization_metric(
                    session, region, inventory, account, metrics, time_generated, addons)
                save_as_file_parquet_metrics(metrics, file_path, generate_parquet_prefix(
                    str(stack()[0][3]), region, f'{account_id}-metrics', idx))
            next_token = response.get('NextMarker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def elbv2_gateway_metrics(file_path, session, region, account, metrics, time_generated):
    next_token = None
    idx = 0
    client = session.client('elbv2', region_name=region)
    account_id = account['account_id']
    while True:
        try:
            inventory = []
            response = client.describe_load_balancers(
                Marker=next_token) if next_token else client.describe_load_balancers()
            for resource in response.get('LoadBalancers', []):
                if resource.get('Type', '').lower() == 'gateway':
                    inventory.append(resource.get(
                        'LoadBalancerArn').split(":loadbalancer/")[-1])
            if inventory:
                metrics = get_resource_utilization_metric(
                    session, region, inventory, account, metrics, time_generated)
                save_as_file_parquet_metrics(metrics, file_path, generate_parquet_prefix(
                    str(stack()[0][3]), region, f'{account_id}-metrics', idx))
            next_token = response.get('NextMarker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
