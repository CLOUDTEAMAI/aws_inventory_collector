from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_eni(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('ec2', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_network_interfaces(
                NextToken=next_token) if next_token else client.describe_network_interfaces()
            for resource in response.get('NetworkInterfaces', []):
                if 'Attachment' in resource:
                    try:
                        resource['Attachment']['AttachTime'] = resource['Attachment']['AttachTime'].isoformat(
                        )
                    except Exception:
                        pass
                arn = f"arn:aws:ec2:{region}:{account_id}:network-interface/{resource['NetworkInterfaceId']}"
                client_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(client_object)
            save_as_file_parquet(inventory, file_path,
                                 generate_parquet_prefix(str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
