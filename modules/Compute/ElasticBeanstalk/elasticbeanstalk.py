from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_elasticbeanstalk(file_path, session, region, time_generated, account):
    """
    This Python function retrieves information about Elastic Beanstalk environments and saves it to a
    Parquet file.

    :param file_path: The `file_path` parameter in the `list_elasticbeanstalk` function is the path
    where the output data will be saved as a Parquet file. This parameter should be a string
    representing the file path where you want to save the data
    :param session: The `session` parameter in the `list_elasticbeanstalk` function is typically an AWS
    session object that is used to create clients for AWS services. It is used to interact with AWS
    Elastic Beanstalk in this case. The session object contains information such as credentials, region,
    and other configuration settings
    :param region: The `region` parameter in the `list_elasticbeanstalk` function is used to specify the
    AWS region where the Elastic Beanstalk resources are located. This parameter is required to create a
    client for the Elastic Beanstalk service in the specified region and to retrieve information about
    the environments in that region
    :param time_generated: Time_generated is a parameter representing the timestamp or time at which the
    inventory data is being generated or collected. It is typically used to track when the inventory
    information was retrieved or processed
    :param account: The `account` parameter in the `list_elasticbeanstalk` function seems to be a
    dictionary containing information about the account. It likely includes keys such as 'account_id'
    and 'account_name'. The function uses the 'account_id' and 'account_name' values from this
    dictionary to perform
    """
    next_token = None
    idx = 0
    client = session.client('elasticbeanstalk', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_environments(
                NextToken=next_token) if next_token else client.describe_environments()
            for resource in response.get('Environments', []):
                if 'DateCreated' in resource:
                    resource['DateCreated'] = resource['DateCreated'].isoformat()
                if 'DateUpdated' in resource:
                    resource['DateUpdated'] = resource['DateUpdated'].isoformat()
                arn = resource['ApplicationArn']
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


def list_elasticbeanstalk_applications(file_path, session, region, time_generated, account):
    """
    This Python function lists Elastic Beanstalk applications and saves the information to a Parquet
    file.

    :param file_path: The `file_path` parameter in the `list_elasticbeanstalk_applications` function is
    the path where the output file will be saved. It is the location on your file system where the
    function will write the data it retrieves about Elastic Beanstalk applications
    :param session: The `session` parameter in the `list_elasticbeanstalk_applications` function is
    typically an instance of a boto3 session that allows you to create service clients for AWS services.
    It is used to interact with the Elastic Beanstalk service in the specified AWS region. You can
    create a session using
    :param region: The `region` parameter in the `list_elasticbeanstalk_applications` function is used
    to specify the AWS region where the Elastic Beanstalk applications are located. This parameter is
    required to create a client session for the Elastic Beanstalk service in the specified region and to
    retrieve information about the applications in
    :param time_generated: The `time_generated` parameter in the `list_elasticbeanstalk_applications`
    function is used to specify the timestamp or time at which the inventory data is being generated or
    collected. This timestamp is typically used for tracking and auditing purposes to know when the
    inventory information was retrieved
    :param account: The `list_elasticbeanstalk_applications` function takes in several parameters:
    """
    next_token = None
    idx = 0
    client = session.client('elasticbeanstalk', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        inventory = []
        response = client.describe_applications(
            NextToken=next_token) if next_token else client.describe_applications()
        for resource in response.get('Applications', []):
            if 'DateCreated' in resource:
                resource['DateCreated'] = resource['DateCreated'].isoformat()
            if 'DateUpdated' in resource:
                resource['DateUpdated'] = resource['DateUpdated'].isoformat()
            arn = resource['ApplicationArn']
            inventory_object = extract_common_info(
                arn, resource, region, account_id, time_generated, account_name)
            inventory.append(inventory_object)
        save_as_file_parquet(inventory, file_path,
                             generate_parquet_prefix(str(stack()[0][3]), region, account_id, idx))
        next_token = response.get('NextToken', None)
        idx = idx + 1
        if not next_token:
            break
