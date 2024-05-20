from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_acl(file_path, session, region, time_generated, account):
    """
    The `list_acl` function retrieves and saves information about network ACLs in AWS using the provided
    parameters.

    :param file_path: The `file_path` parameter in the `list_acl` function is the path where the output
    files will be saved. It should be a string representing the directory or file path where the
    generated output files will be stored
    :param session: The `session` parameter in the `list_acl` function is an AWS session object that is
    used to create a client for the EC2 service in a specific region. It is typically created using the
    `boto3.Session` class and is used to make API calls to AWS services
    :param region: The `region` parameter in the `list_acl` function is used to specify the AWS region
    in which the network ACLs (Access Control Lists) should be listed. This parameter determines the
    region where the AWS SDK client will operate and retrieve the network ACL information from. It
    should be a string representing
    :param time_generated: Time_generated is a timestamp indicating when the ACL list is generated. It
    is used as a parameter in the function to track the time at which the ACL information is retrieved
    :param account: The `account` parameter in the `list_acl` function seems to be a dictionary
    containing information about an AWS account. It likely includes the account ID and account name. The
    function uses the account ID and account name to construct ARNs and perform other operations related
    to listing network ACLs in the specified
    """
    next_token = None
    idx = 0
    client = session.client('ec2', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_network_acls(
                NextToken=next_token) if next_token else client.describe_network_acls()
            for resource in response.get('NetworkAcls', []):
                arn = f"arn:aws:ec2:{region}:{account_id}:network-acl/{resource['NetworkAclId']}"
                client_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(client_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
