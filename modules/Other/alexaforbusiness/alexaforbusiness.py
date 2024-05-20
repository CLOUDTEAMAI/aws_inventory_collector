from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_alexaforbusiness(file_path, session, region, time_generated, account):
    """
    This Python function lists Alexa for Business skills and saves the information to a Parquet file.

    :param file_path: The `file_path` parameter in the `list_alexaforbusiness` function is the path
    where the inventory data will be saved as a Parquet file. It is a string that represents the
    location where the file will be stored on the file system
    :param session: The `session` parameter in the `list_alexaforbusiness` function is typically an
    instance of a boto3 session that is used to create a client for interacting with AWS services. It
    allows you to configure credentials, region, and other settings for making API calls to AWS services
    like Alexa for
    :param region: The `region` parameter in the `list_alexaforbusiness` function refers to the AWS
    region where the Alexa for Business service is located. This parameter is used to specify the region
    name when creating a client session for the Alexa for Business service. It is important to provide
    the correct region where
    :param time_generated: The `time_generated` parameter in the `list_alexaforbusiness` function is
    used to specify the timestamp or time at which the inventory data is being generated or collected.
    This timestamp is important for tracking when the inventory information was retrieved and can be
    useful for auditing or monitoring purposes. It helps
    :param account: The `account` parameter in the `list_alexaforbusiness` function seems to be a
    dictionary containing information about the account. It likely includes the keys `account_id` and
    `account_name`
    """
    next_token = None
    idx = 0
    client = session.client('alexaforbusiness', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_skills(
                NextToken=next_token) if next_token else client.list_skills()
            for resource in response.get('Skills', []):
                arn = f"arn:aws:alexaforbusiness:{region}:{account_id}:skill/{resource['SkillId']}"
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
