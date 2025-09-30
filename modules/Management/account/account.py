from os import getenv
from boto3 import Session, client


def complete_aws_account(account):
    """
    The function `complete_aws_account` takes an AWS account dictionary and returns a new dictionary
    with default values for missing keys.

    :param account: The `complete_aws_account` function takes an `account` dictionary as input and
    returns a new dictionary with the following keys:
    :return: The function `complete_aws_account` is returning a dictionary with the keys 'account_id',
    'account_role', and 'account_name'. The values for 'account_id' are taken from the input `account`
    dictionary, while the values for 'account_role' default to 'Cloudteam-FinOps' if not present in the
    input `account` dictionary. The value for 'account_name'
    """
    return {'account_id': account['account_id'], 'account_role': account.get('account_role', ''), 'account_name': account.get('account_name', '')}


def regions_enabled(session):
    """
    The function `regions_enabled` returns a list of AWS regions that are enabled for a given session.

    :param session: It looks like the `regions_enabled` function is designed to return a list of AWS
    regions that are enabled for a given session. The function seems to be hardcoding a list of AWS
    regions that are enabled
    :return: The function `regions_enabled(session)` returns a list of AWS regions that are enabled for
    the given session.
    """
    default_regions = ['us-east-2',
                       'us-east-1',
                       'us-west-1',
                       'us-west-2',
                       'af-south-1',
                       'ap-east-1',
                       'ap-south-2',
                       'ap-southeast-3',
                       'ap-southeast-4',
                       'ap-south-1',
                       'ap-northeast-3',
                       'ap-northeast-2',
                       'ap-southeast-1',
                       'ap-southeast-2',
                       'ap-northeast-1',
                       'ca-central-1',
                       'ca-west-1',
                       'eu-central-1',
                       'eu-west-1',
                       'eu-west-2',
                       'eu-south-1',
                       'eu-west-3',
                       'eu-south-2',
                       'eu-north-1',
                       'eu-central-2',
                       'il-central-1',
                       'me-south-1',
                       'me-central-1',
                       'sa-east-1']
    try:
        ec2_client = session.client('ec2')
        regions = [region['RegionName']
                   for region in ec2_client.describe_regions()['Regions']]
        if regions:
            return regions
    except Exception:
        return default_regions


def get_credentials_assume_role(account_id, role_name="", region='us-east-1', external_id=None):
    """
    The function `get_credentials_assume_role` assumes an AWS IAM role in a specified account and
    returns the temporary credentials.

    :param account_id: The `account_id` parameter is the unique identifier for the AWS account for which
    you want to assume a role. It is used to construct the ARN (Amazon Resource Name) of the role that
    you want to assume within that account
    :param role_name: The `role_name` parameter in the `get_credentials_assume_role` function is a
    string that represents the name of the IAM role that you want to assume in the AWS account specified
    by `account_id`. By default, the `role_name` is set to "Cloudteam-FinOps", defaults to
    Cloudteam-FinOps (optional)
    :param external_id: The `external_id` parameter in the `get_credentials_assume_role` function is
    used for providing an external identifier that allows you to assume a role in another account. This
    is an optional parameter and can be used for additional security when you are assuming a role in
    another AWS account
    :return: The function `get_credentials_assume_role` is returning the temporary credentials obtained
    by assuming the specified IAM role using the AWS Security Token Service (STS).
    """
    try:
        role_arn = 'arn:aws:iam::' + account_id + ':role/' + role_name
        sts_client = client('sts')
        assumed_role_object = sts_client.assume_role(
            RoleArn=role_arn,
            RoleSessionName="CLOUDTEAM-FINOPS"
            # ExternalId=external_id
        )
        credentials = assumed_role_object['Credentials']
        return credentials
    except Exception as e:
        print(e)
        # log.info(f"An error occurred while assuming the role: {e}")
        return None


def get_aws_session(account_id, region='us-east-1', role_name="", sso_mode=False):
    """
    This function uses the `get_credentials_assume_role` function to get temporary credentials for the specified role, and
    then uses those credentials to create a new `boto3` session

    :param account_id: The AWS account ID of the account you want to assume a role in
    :param role_name: The name of the role you want to assume
    :param external_id: the external id from CloudHiro
    :return: A session object that can be used to make API calls to the account specified in the role_name parameter.
        external_id:
    """
    try:
        credentials = None
        if sso_mode:
            aws_session = Session(region_name=region)
            return aws_session
        if role_name:
            credentials = get_credentials_assume_role(
                account_id, role_name, region)
        if credentials is not None:
            aws_session = Session(aws_access_key_id=credentials['AccessKeyId'],
                                  aws_secret_access_key=credentials['SecretAccessKey'],
                                  aws_session_token=credentials['SessionToken'],
                                  region_name=region
                                  )
            return aws_session
        else:
            aws_session = Session(region_name=region)
            return aws_session
    except Exception:
        # log.error(f"Error occurred get credentials: {e}")
        return None
