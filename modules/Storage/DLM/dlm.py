from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_dlm(file_path, session, region, time_generated, account):
    """
    The function `list_dlm` retrieves and processes AWS DLM lifecycle policies, saving the information
    to a Parquet file.

    :param file_path: The `file_path` parameter in the `list_dlm` function represents the path where the
    output file will be saved. This should be a valid file path on your system where you want to store
    the data retrieved by the function
    :param session: The `session` parameter in the `list_dlm` function is an AWS session object that is
    used to create a client for the AWS Data Lifecycle Manager (DLM) service. It is typically created
    using the `boto3.Session` class and is used to interact with AWS services within a
    :param region: The `region` parameter in the `list_dlm` function is used to specify the AWS region
    in which the Data Lifecycle Manager (DLM) client will operate. This parameter determines the region
    where the DLM API calls will be made to retrieve information about lifecycle policies. It should be
    a string
    :param time_generated: The `time_generated` parameter in the `list_dlm` function is used to specify
    the timestamp when the inventory data is generated. This timestamp is typically used for tracking
    and auditing purposes to know when the inventory information was collected or updated. It helps in
    maintaining a record of when the inventory data was
    :param account: The `account` parameter in the `list_dlm` function seems to be a dictionary
    containing information about an AWS account. It likely includes keys such as 'account_id' and
    'account_name' to identify the AWS account. This information is used within the function to retrieve
    and process lifecycle policies related
    """
    next_token = None
    idx = 0
    client = session.client('dlm', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.get_lifecycle_policies(DefaultPolicyType='ALL')
            for resource in response.get('Policies', []):
                resource_response = client.get_lifecycle_policy(
                    PolicyId=resource['PolicyId']
                )
                policy = resource_response.get('Policy')
                if 'DateCreated' in policy:
                    policy['DateCreated'] = policy['DateCreated'].isoformat()
                if 'DateModified' in policy:
                    policy['DateModified'] = policy['DateModified'].isoformat(
                    )
                arn = f"arn:aws:dlm:{region}:{account_id}:policy/{policy['PolicyId']}"
                inventory_object = extract_common_info(
                    arn, policy, region, account_id, time_generated, account_name)
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
