import boto3

def regions_enabled(session):
    account = session.client('account') 
    sts = session.client('sts')
    regions = account.list_regions()
    available_regions  = session.get_available_regions('ec2')
    regions_enabled = [region for region in regions['Regions'] if region['RegionOptStatus'] in ['ENABLED_BY_DEFAULT', 'ENABLED']]
    regions_name = [name['RegionName'] for name in regions_enabled]
    return available_regions



def get_credentials_assume_role(account_id, role_name, external_id=None):
    try:
        role_arn = 'arn:aws:iam::' + account_id + ':role/' + role_name
        sts_client = boto3.client('sts')
        assumed_role_object = sts_client.assume_role(
            RoleArn=role_arn,
            DurationSeconds=3600,
            RoleSessionName="Recommender",
            # ExternalId=external_id
        )
        credentials = assumed_role_object['Credentials']
        return credentials
    except Exception as e:
        # log.info(f"An error occurred while assuming the role: {e}")
        return None


def get_aws_session(account_id,region=None):
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
        role_name = 'Cloudteam-FinOps'
        credentials = get_credentials_assume_role(
            account_id, role_name)
        if credentials is not None:
            aws_session = boto3.Session(aws_access_key_id=credentials['AccessKeyId'],
                                        aws_secret_access_key=credentials['SecretAccessKey'],
                                        aws_session_token=credentials['SessionToken'],
                                        region_name=region
                                        )
            return aws_session
    except Exception as e:
        # log.error(f"Error occurred get credentials: {e}")
        return None
    




   