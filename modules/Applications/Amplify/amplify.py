from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_amplify(file_path, session, region, time_generated, account):
    """
    The function `list_amplify` retrieves a list of Amplify apps using the AWS Amplify client and saves
    the information to a Parquet file.

    :param file_path: The `file_path` parameter in the `list_amplify` function is the path where the
    output file will be saved. It should be a string representing the file path where the Parquet file
    will be stored
    :param session: The `session` parameter in the `list_amplify` function is typically an instance of a
    boto3 session that is used to create a client for the AWS Amplify service in a specific region. It
    allows you to interact with AWS services using the credentials and configuration provided in the
    session
    :param region: Region is a string representing the geographical region where the AWS Amplify
    resources are located. It specifies the AWS region where the Amplify client will be created to
    interact with the Amplify service in that specific region
    :param time_generated: Time_generated is a timestamp indicating when the data was generated or
    collected. It is typically used for tracking the timing of data retrieval or processing operations
    :param account: The `account` parameter in the `list_amplify` function seems to be a dictionary
    containing information about an account. It likely includes the account ID (`account_id`) and the
    account name (`account_name`). The function uses this information to interact with the AWS Amplify
    service in the specified region
    """
    next_token = None
    idx = 0
    client = session.client('amplify', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_apps(
                nextToken=next_token) if next_token else client.list_apps()
            for resource in response.get('apps', []):
                arn = resource['appArn']
                object_client = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(object_client)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('nextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break

# def list_amplifyuibuilder(file_path,session,region):
#     client_boto = session.client('amplifyuibuilder',region_name=region)
#     sts = session.client('sts')
#     account_id = sts.get_caller_identity()["Account"]
#     amplifyuibuilder_list = client_boto.list_apps()
#     amplify_list = []
#     region          = region
#     if len(amplifyuibuilder_list['entities']) != 0:
#         for i in amplifyuibuilder_list['entities']:
#             arn = i['appArn']
#             object_client = extract_common_info(arn,i,region,account_id)
#             amplify_list.append(object_client)
#         save_as_file_parquet(amplify_list,file_path,generate_parquet_prefix(__file__,region,account_id))
#     #
