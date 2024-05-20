from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_ec2_reservations(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('ec2', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_reserved_instances(
                NextToken=next_token) if next_token else client.describe_reserved_instances()
            for resource in response.get('ReservedInstances', []):
                arn = f"arn:aws:ec2:{region}:{account_id}:reserved-instances/{resource.get('ReservedInstancesId', '')}"
                if 'End' in resource:
                    resource['End'] = resource['End'].isoformat()
                if 'Start' in resource:
                    resource['Start'] = resource['Start'].isoformat()
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


def list_rds_reservations(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('rds', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_reserved_db_instances(
                Marker=next_token) if next_token else client.describe_reserved_db_instances()
            for resource in response.get('ReservedDBInstances', []):
                arn = f"arn:aws:rds:{region}:{account_id}:reserved-instances/{resource.get('ReservedDBInstanceId', '')}"
                if 'End' in resource:
                    resource['End'] = resource['End'].isoformat()
                if 'Start' in resource:
                    resource['Start'] = resource['Start'].isoformat()
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('Marker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def list_opensearch_reservations(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('opensearch', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_reserved_instances(
                NextToken=next_token) if next_token else client.describe_reserved_instances()
            for resource in response.get('ReservedInstances', []):
                arn = f"arn:aws:opensearch:{region}:{account_id}:reserved-instances/{resource.get('ReservedInstanceId', '')}"
                if 'StartTime' in resource:
                    resource['StartTime'] = resource['StartTime'].isoformat()
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


def list_elasticsearch_reservations(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('es', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_reserved_elasticsearch_instances(
                NextToken=next_token) if next_token else client.describe_reserved_elasticsearch_instances()
            for resource in response.get('ReservedElasticsearchInstances', []):
                arn = f"arn:aws:elasticsearch:{region}:{account_id}:reserved-instances/{resource.get('ReservedElasticsearchInstanceId', '')}"
                if 'StartTime' in resource:
                    resource['StartTime'] = resource['StartTime'].isoformat()
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


def list_elasticcache_reservations(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('elasticache', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_reserved_cache_nodes(
                Marker=next_token) if next_token else client.describe_reserved_cache_nodes()
            for resource in response.get('ReservedCacheNodes', []):
                arn = resource.get(
                    'ReservationARN', f"arn:aws:rds:{region}:{account_id}:reserved-instances/{resource.get('ReservedCacheNodeId', '')}")
                if 'StartTime' in resource:
                    resource['StartTime'] = resource['StartTime'].isoformat()
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('Marker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def list_memorydb_reservations(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('memorydb', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_reserved_nodes(
                NextToken=next_token) if next_token else client.describe_reserved_nodes()
            for resource in response.get('ReservedNodes', []):
                arn = resource.get(
                    'ARN', f"arn:aws:memorydb:{region}:{account_id}:reserved-instances/{resource.get('ReservedNodeId', '')}")
                if 'StartTime' in resource:
                    resource['StartTime'] = resource['StartTime'].isoformat()
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


def list_redshift_reservations(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('redshift', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_reserved_nodes(
                Marker=next_token) if next_token else client.describe_reserved_nodes()
            for resource in response.get('ReservedNodes', []):
                arn = resource.get(
                    'ARN', f"arn:aws:redshift:{region}:{account_id}:reserved-instances/{resource.get('ReservedNodeId', '')}")
                if 'StartTime' in resource:
                    resource['StartTime'] = resource['StartTime'].isoformat()
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('Marker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
