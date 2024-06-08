from boto3 import Session, client


def get_credentials_assume_role(account_id, role_name="Cloudteam-FinOps", region='us-east-1', external_id=None):
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
            RoleSessionName="CLOUDTEAM-FINOPS",
            # ExternalId=external_id
        )
        credentials = assumed_role_object['Credentials']
        return credentials
    except Exception as e:
        print(e)
        # log.info(f"An error occurred while assuming the role: {e}")
        return None


def get_aws_session(account_id, region='us-east-1', role_name="Cloudteam-FinOps"):
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
        credentials = get_credentials_assume_role(
            account_id, role_name, region)
        if credentials is not None:
            aws_session = Session(aws_access_key_id=credentials['AccessKeyId'],
                                  aws_secret_access_key=credentials['SecretAccessKey'],
                                  aws_session_token=credentials['SessionToken'],
                                  region_name=region
                                  )
            return aws_session
    except Exception:
        # log.error(f"Error occurred get credentials: {e}")
        return None


inv = []
next_token = None
session = get_aws_session('598939805063', 'af-south-1')

client = session.client('cloudwatch')
while True:
    try:
        response = client.list_metrics(
            NextToken=next_token, Namespace='CWAgent') if next_token else client.list_metrics(Namespace='CWAgent')
        inv.append(response)
        next_token = response.get('NextToken', None)
        if not next_token:
            break
    except Exception:
        break

print(response)
