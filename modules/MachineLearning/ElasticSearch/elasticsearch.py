from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_elasticsearch_domains(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('es', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    inventory = []
    names = []
    response = client.list_domain_names()
    for resource in response.get('DomainNames', []):
        names.append(resource['DomainName'])
    while True:
        try:
            inventory = []
            response = client.describe_elasticsearch_domains(
                DomainNames=names)
            for resource in response.get('DomainStatusList', []):
                arn = resource.get('ARN', '')
                if 'AutomatedUpdateDate' in resource.get('ServiceSoftwareOptions', {}):
                    resource['ServiceSoftwareOptions']['AutomatedUpdateDate'] = resource[
                        'ServiceSoftwareOptions']['AutomatedUpdateDate'].isoformat()
                if 'AnonymousAuthDisableDate' in resource.get('AdvancedSecurityOptions', {}):
                    resource['AdvancedSecurityOptions']['AnonymousAuthDisableDate'] = resource[
                        'AdvancedSecurityOptions']['AnonymousAuthDisableDate'].isoformat()
                if 'StartTime' in resource.get('ChangeProgressDetails', {}):
                    resource['ChangeProgressDetails']['StartTime'] = resource[
                        'ChangeProgressDetails']['StartTime'].isoformat()
                if 'LastUpdatedTime' in resource.get('ChangeProgressDetails', {}):
                    resource['ChangeProgressDetails']['LastUpdatedTime'] = resource[
                        'ChangeProgressDetails']['LastUpdatedTime'].isoformat()

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
