from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_vpc(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('ec2', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_vpcs(
                NextToken=next_token) if next_token else client.describe_vpcs()
            for resource in response.get('Vpcs', []):
                arn = f"arn:aws:ec2:{region}:{account_id}:vpc/{resource['VpcId']}"
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


def list_vpc_endpoint(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('ec2', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_vpc_endpoints(
                NextToken=next_token) if next_token else client.describe_vpc_endpoints()
            for resource in response.get('VpcEndpoints', []):
                resource['CreationTimestamp'] = resource['CreationTimestamp'].isoformat()
                arn = f"arn:aws:ec2:{region}:{account_id}:vpc-endpoint/{resource['VpcEndpointId']}"
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


def list_vpc_endpoint_services(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('ec2', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_vpc_endpoint_connections(
                NextToken=next_token) if next_token else client.describe_vpc_endpoint_connections()
            for resource in response.get('VpcEndpointConnections', []):
                resource['CreationTimestamp'] = resource['CreationTimestamp'].isoformat()
                arn = f"arn:aws:ec2:{region}:{account_id}:vpc-endpoint/{resource['VpcEndpointId']}"
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


def list_vpc_peering(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('ec2', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_vpc_peering_connections(
                NextToken=next_token) if next_token else client.describe_vpc_peering_connections()
            for resource in response.get('VpcPeeringConnections', []):
                resource['ExpirationTime'] = resource['ExpirationTime'].isoformat()
                arn = f"arn:aws:ec2:{region}:{account_id}:vpc-peering-connection/{resource['VpcPeeringConnectionId']}"
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


def list_vpclattice(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('vpc-lattice', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_services(
                nextToken=next_token) if next_token else client.list_services()
            for resource in response.get('items', []):
                resource['createdAt'] = resource['createdAt'].isoformat()
                resource['lastUpdatedAt'] = resource['lastUpdatedAt'].isoformat()
                arn = resource['arn']
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('nextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
