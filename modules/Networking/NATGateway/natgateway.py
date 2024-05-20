from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_natgateway(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('ec2', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_nat_gateways(
                NextToken=next_token) if next_token else client.describe_nat_gateways()
            for resource in response.get('NatGateways', []):
                if 'CreateTime' in resource:
                    resource['CreateTime'] = resource['CreateTime'].isoformat()
                if 'DeleteTime' in resource:
                    resource['DeleteTime'] = resource['DeleteTime'].isoformat()
                if 'ProvisionedBandwidth' in resource:
                    resource['ProvisionedBandwidth']['ProvisionTime'] = resource['ProvisionedBandwidth']['ProvisionTime'].isoformat()
                    resource['ProvisionedBandwidth']['RequestTime'] = resource['ProvisionedBandwidth']['RequestTime'].isoformat()

                arn = f"arn:aws:ec2:{region}:{account_id}:natgateway/{resource['NatGatewayId']}"
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
