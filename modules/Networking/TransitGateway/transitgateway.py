from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_transitgateway(file_path, session, region, time_generated, account, boto_config):
    next_token = None
    idx = 0
    client = session.client('ec2', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_transit_gateways(
                NextToken=next_token) if next_token else client.describe_transit_gateways()
            for resource in response.get('TransitGateways', []):
                arn = resource.get('TransitGatewayArn', '')
                if 'CreationTime' in resource:
                    resource['CreationTime'] = resource['CreationTime'].isoformat()
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
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


def list_transitgateway_attachments(file_path, session, region, time_generated, account, boto_config):
    next_token = None
    idx = 0
    client = session.client('ec2', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_transit_gateway_attachments(
                NextToken=next_token) if next_token else client.describe_transit_gateway_attachments()
            for resource in response.get('TransitGatewayAttachments', []):
                arn = f"arn:aws:ec2:{region}:{account_id}:transit-gateway-attachment/{resource.get('TransitGatewayAttachmentId')}"
                if 'CreationTime' in resource:
                    resource['CreationTime'] = resource['CreationTime'].isoformat()
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
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
