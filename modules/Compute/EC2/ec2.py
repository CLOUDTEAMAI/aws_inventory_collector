from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_ec2(file_path, session, region, time_generated, account, boto_config):
    """
    The function `list_ec2` retrieves information about EC2 instances, formats the data, and saves it as
    a Parquet file.

    :param file_path: The `file_path` parameter in the `list_ec2` function refers to the path where the
    output file will be saved. This should be a valid file path on your system where you want to store
    the data retrieved from the EC2 instances
    :param session: The `session` parameter in the `list_ec2` function is typically an instance of
    `boto3.Session` class that represents the connection to AWS services. It is used to create service
    clients like `ec2` in this case, which allows you to make API calls to AWS services
    :param region: The `region` parameter in the `list_ec2` function refers to the AWS region where the
    EC2 instances are located. This parameter is used to specify the region when creating an EC2 client
    in the AWS SDK. It is important to provide the correct region where your EC2 instances are deployed
    :param time_generated: The `time_generated` parameter in the `list_ec2` function likely represents
    the timestamp or datetime when the inventory list of EC2 instances is being generated. This
    parameter is used to track when the inventory data was collected or created. It is important for
    maintaining a record of when the inventory information was
    :param account: The `account` parameter in the `list_ec2` function seems to be a dictionary
    containing information about an AWS account. It likely includes keys such as 'account_id' and
    'account_name' which are used within the function to identify and process resources associated with
    that specific AWS account
    """
    next_token = None
    idx = 0
    client = session.client('ec2', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account.get('account_name', '')).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_instances(
                NextToken=next_token) if next_token else client.describe_instances()
            for resource in response.get('Reservations', []):
                for instance in resource['Instances']:
                    ebs_counter = 0
                    eni_counter = 0
                    eiaa_counter = 0
                    instance['LaunchTime'] = instance['LaunchTime'].isoformat()
                    if 'UsageOperationUpdateTime' in instance:
                        instance['UsageOperationUpdateTime'] = instance['UsageOperationUpdateTime'].isoformat(
                        )
                    for eiaa in instance.get('ElasticInferenceAcceleratorAssociations', []):
                        if 'ElasticInferenceAcceleratorAssociationTime' in eiaa:
                            instance['ElasticInferenceAcceleratorAssociations'][eiaa_counter]['ElasticInferenceAcceleratorAssociationTime'] = eiaa['ElasticInferenceAcceleratorAssociationTime'].isoformat(
                            )
                        eiaa_counter = eiaa_counter + 1

                    for device in instance.get('BlockDeviceMappings', []):
                        if 'Ebs' in device and 'AttachTime' in device['Ebs']:
                            instance['BlockDeviceMappings'][ebs_counter]['Ebs']['AttachTime'] = device['Ebs']['AttachTime'].isoformat(
                            )
                        ebs_counter = ebs_counter + 1

                    for interface in instance.get('NetworkInterfaces', []):
                        if 'Attachment' in interface and 'AttachTime' in interface['Attachment']:
                            instance['NetworkInterfaces'][eni_counter]['Attachment']['AttachTime'] = interface['Attachment']['AttachTime'].isoformat(
                            )
                        eni_counter = eni_counter + 1
                    arn = f"arn:aws:ec2:{region}:{account_id}:instance/{instance['InstanceId']}"
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


def list_ami(file_path, session, region, time_generated, account, boto_config):
    """
    The function `list_ami` retrieves information about AMIs owned by the specified account in the given
    region and saves the data in a Parquet file.

    :param file_path: The `file_path` parameter in the `list_ami` function is the path where the output
    file will be saved. It should be a string representing the file path where the Parquet file will be
    stored
    :param session: The `session` parameter in the `list_ami` function is an AWS session object that is
    used to create a client for the EC2 service in a specific region. This session object typically
    contains the credentials and configuration needed to interact with AWS services
    :param region: The `region` parameter in the `list_ami` function is used to specify the AWS region
    in which the Amazon EC2 images (AMIs) are to be listed. This parameter determines the region where
    the `describe_images` API call will be made to retrieve information about the AMIs owned
    :param time_generated: Time when the AMI list is generated. It is used as a reference timestamp for
    the inventory
    :param account: The `account` parameter in the `list_ami` function seems to be a dictionary
    containing information about the AWS account. It likely includes the keys `account_id` and
    `account_name`. The `account_id` is the unique identifier for the AWS account, and the
    `account_name` is
    """
    next_token = None
    idx = 0
    client = session.client('ec2', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_images(Owners=['self'],
                                              NextToken=next_token) if next_token else client.describe_images(Owners=['self'])
            for resource in response.get('Images', []):
                arn = f"arn:aws:ec2:{region}:{account_id}:image/{resource['ImageId']}"
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
