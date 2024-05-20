from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_waf(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('waf', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_web_acls(
                NextMarker=next_token) if next_token else client.list_web_acls()
            for resource in response.get('WebACLs'):
                arn = resource['ARN']
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextMarker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def list_wafv2(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('wafv2', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    scopes = ['CLOUDFRONT', 'REGIONAL']
    for scope in scopes:
        while True:
            try:
                inventory = []
                response = client.list_web_acls(
                    Scope=scope, NextMarker=next_token) if next_token else client.list_web_acls(Scope=scope)
                for resource in response.get('WebACLs'):
                    arn = resource['ARN']
                    inventory_object = extract_common_info(
                        arn, resource, region, account_id, time_generated, account_name)
                    inventory.append(inventory_object)
                save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                    str(stack()[0][3]), region, account_id, idx))
                next_token = response.get('NextMarker', None)
                idx = idx + 1
                if not next_token:
                    break
            except Exception as e:
                print(e)
                break


def list_wafregional(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('wafregional', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_web_acls(
                NextMarker=next_token) if next_token else client.list_web_acls()
            for resource in response.get('WebACLs'):
                arn = resource['ARN']
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextMarker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
