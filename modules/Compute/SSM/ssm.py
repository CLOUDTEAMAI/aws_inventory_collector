from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_ssm(file_path, session, region, time_generated, account, boto_config):
    """
    The function `list_ssm` retrieves and saves information about SSM parameters to a Parquet file.

    :param file_path: The `file_path` parameter in the `list_ssm` function is the path where the output
    file will be saved. It is the location on the file system where the Parquet file containing the SSM
    inventory data will be stored
    :param session: The `session` parameter is typically an instance of `boto3.Session` class that
    represents your AWS credentials and configuration. It is used to create service clients for AWS
    services like SSM (Systems Manager) in this case. The `client` method of the `session` object is
    used to
    :param region: The `region` parameter in the `list_ssm` function is used to specify the AWS region
    in which the AWS Systems Manager (SSM) client will be created. This region is where the function
    will retrieve information about SSM parameters using the SSM client
    :param time_generated: Time_generated is a timestamp indicating when the inventory data was
    generated. It is used in the function to track the time at which the inventory information is
    collected and processed
    :param account: The `account` parameter in the `list_ssm` function seems to be a dictionary
    containing information about an account. It likely has the following structure:
    """
    next_token = None
    idx = 0
    client = session.client('ssm', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_parameters(
                NextToken=next_token) if next_token else client.describe_parameters()
            for resource in response.get('Parameters', []):
                resource['LastModifiedDate'] = resource['LastModifiedDate'].isoformat()
                name = ""
                if resource.get('Name', '').startswith('/'):
                    name = resource.get('Name', '')[1:-1]
                arn = resource.get(
                    'ARN', f"arn:aws:ssm:{region}:{account_id}:parameter/{name}")
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
