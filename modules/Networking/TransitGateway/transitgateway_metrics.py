from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def transitgateway_metrics(file_path, session, region, account, metrics, time_generated):
    next_token = None
    idx = 0
    account_id = account['account_id']
    while True:
        try:
            client = session.client('ec2', region_name=region)
            inventory = []
            response = client.describe_transit_gateways(
                NextToken=next_token) if next_token else client.describe_transit_gateways()
            for resource in response.get('TransitGateways', []):
                inventory.append(resource['TransitGatewayId'])
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


def transitgateway_attachments_metrics(file_path, session, region, account, metrics, time_generated):
    next_token = None
    idx = 0
    account_id = account['account_id']
    while True:
        try:
            gw_idx = 0
            client = session.client('ec2', region_name=region)
            inventory = []
            addons = {"type": "transitgateway"}
            addons['nodes'] = []
            tgw_idx = {}
            response = client.describe_transit_gateway_attachments(
                NextToken=next_token) if next_token else client.describe_transit_gateway_attachments()
            for resource in response.get('TransitGatewayAttachments', []):
                inventory.append(resource['TransitGatewayId'])
                if resource['TransitGatewayId'] in tgw_idx:
                    addons['nodes'][tgw_idx[resource['TransitGatewayId']]]['nodes'].append(
                        resource.get('TransitGatewayAttachmentId', ''))
                else:
                    tgw_idx[resource['TransitGatewayId']] = gw_idx
                    addons['nodes'].append(
                        {"TransitGatewayId": resource['TransitGatewayId'], "nodes": []})
                    addons['nodes'][tgw_idx[resource['TransitGatewayId']]]['nodes'].append(
                        resource.get('TransitGatewayAttachmentId', ''))
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
