from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_vpn_gateway(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('ec2', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_vpn_gateways(
                NextToken=next_token) if next_token else client.describe_vpn_gateways()
            for resource in response.get('VpnGateways', []):
                arn = f"arn:aws:ec2:{region}:{account_id}:vpn-gateway/{resource['VpnGatewayId']}"
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


def list_vpn_connections(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('ec2', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_vpn_connections(
                NextToken=next_token) if next_token else client.describe_vpn_connections()
            for resource in response.get('VpnConnections', []):
                telemetry_idx = 0
                arn = f"arn:aws:ec2:{region}:{account_id}:vpn-connection/{resource['VpnConnectionId']}"
                for telemetry in resource.get('VgwTelemetry', []):
                    resource['VgwTelemetry'][telemetry_idx]['LastStatusChange'] = telemetry['LastStatusChange'].isoformat()
                    telemetry = telemetry_idx + 1
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


def list_clientvpn(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('ec2', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_client_vpn_endpoints(
                NextToken=next_token) if next_token else client.describe_client_vpn_endpoints()
            for resource in response.get('ClientVpnEndpoints', []):
                arn = f"arn:aws:ec2:{region}:{account_id}:client-vpn-endpoint/{resource['ClientVpnEndpointId']}"
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


def list_clientvpn_connections(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    connection_idx = 0
    client = session.client('ec2', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_client_vpn_endpoints(
                NextToken=next_token) if next_token else client.describe_client_vpn_endpoints()
            for resource in response.get('ClientVpnEndpoints', []):
                connection_next_token = None
                while True:
                    try:
                        connection_response = client.describe_client_vpn_connections(
                            ClientVpnEndpointId=resource['ClientVpnEndpointId'], NextToken=connection_next_token) if connection_next_token else client.describe_client_vpn_connections(ClientVpnEndpointId=resource['ClientVpnEndpointId'])
                        for connection in connection_response.get('Connections'):
                            arn = f"arn:aws:client-vpn:{account_id}:{region}:endpoint/{resource['ClientVpnEndpointId']}/assocation/{connection['ConnectionId']}"
                            inventory_object = extract_common_info(
                                arn, resource, region, account_id, time_generated, account_name)
                            inventory.append(inventory_object)
                        connection_next_token = connection_response.get(
                            'NextToken', None)
                        connection_idx = connection_idx + 1
                        if not next_token:
                            break
                    except Exception as e:
                        print(e)
                        break
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
