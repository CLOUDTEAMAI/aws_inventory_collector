from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_volumes(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function lists volumes in an AWS account and saves the information in a Parquet file.

    :param file_path: The `file_path` parameter in the `list_volumes` function is the path where the
    inventory data will be saved as a Parquet file. It is the location on the file system where the
    output file will be stored
    :param session: The `session` parameter in the `list_volumes` function is typically an AWS session
    object that is used to create clients for various AWS services. It is commonly created using the
    `boto3.Session` class and is used to interact with AWS services in a specific region
    :param region: The `region` parameter in the `list_volumes` function is used to specify the AWS
    region in which the EC2 volumes are located. This parameter is required to create an EC2 client in
    the specified region and to generate the ARN (Amazon Resource Name) for each volume based on the
    :param time_generated: Time_generated is a parameter that represents the timestamp or time at which
    the volumes inventory is being generated. It is used to track when the inventory data was collected
    or created
    :param account: The `account` parameter seems to be a dictionary containing information about an AWS
    account. It likely includes keys such as 'account_id' and 'account_name'. This information is used
    in the `list_volumes` function to retrieve and process volume data from AWS EC2 instances associated
    with the specified account
    """
    next_token = None
    idx = 0
    client = session.client('ec2', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            attachments_counter = 0
            response = client.describe_volumes(
                NextToken=next_token) if next_token else client.describe_volumes()
            for resource in response.get('Volumes', []):
                for vol in resource.get('Attachments', []):
                    resource['Attachments'][attachments_counter]['AttachTime'] = vol['AttachTime'].isoformat(
                    )

                resource['CreateTime'] = resource['CreateTime'].isoformat()
                arn = f"arn:aws:ec2:{region}:{account_id}:volume/{resource['VolumeId']}"
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
