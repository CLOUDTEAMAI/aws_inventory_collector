from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_route53(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('route53', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_hosted_zones(
                Marker=next_token) if next_token else client.list_hosted_zones()
            for resource in response.get('HostedZones', []):
                arn = f"arn:aws:route53::{':'.join(resource['Id'].split('/'))}"
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path,
                                 generate_parquet_prefix(str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextMarker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def list_route53_resolver(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('route53resolver', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_resolver_endpoints(
                NextToken=next_token) if next_token else client.list_resolver_endpoints()
            for resource in response.get('ResolverEndpoints', []):
                arn = resource.get('Arn', '')
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path,
                                 generate_parquet_prefix(str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def list_route53_profiles(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('route53profiles', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_profiles(
                NextToken=next_token) if next_token else client.list_profiles()
            for resource in response.get('ProfileSummaries', []):
                arn = resource.get('Arn')
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


def list_route53_profiles_associations(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('route53profiles', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_profile_associations(
                NextToken=next_token) if next_token else client.list_profile_associations()
            for resource in response.get('ProfileAssociations', []):
                if 'CreationTime' in resource:
                    resource['CreationTime'] = resource['CreationTime'].isoformat(
                    )
                if 'ModificationTime' in resource:
                    if 'CreatedAt' in resource['ModificationTime']:
                        resource['ModificationTime'] = resource['LastUpdate'].isoformat(
                        )
                arn = f"arn:aws:route53profiles:{region}:{account_id}:profile-association/{resource['id']}"
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
