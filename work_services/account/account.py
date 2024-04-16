import boto3
# import aioboto3

def regions_enabled(session):
    regions_name = []
    if session is not None:
        account = session.client('account') 
        sts = session.client('sts') 
        account_id = sts.get_caller_identity()['Account']
        try:
            # regions = account.list_regions()
            available_regions  = session.get_available_regions('ec2')
            # regions_enabled = [region for region in regions['Regions'] if region['RegionOptStatus'] in ['ENABLED_BY_DEFAULT', 'ENABLED']]
            # regions_name = [name['RegionName'] for name in regions_enabled]
        except Exception as ex:
            print(ex)
        regions_name = ['us-east-1', 'us-east-2', 'us-west-1',
                        'us-west-2', 'af-south-1', 'ap-east-1',
                        'ap-south-1', 'ap-northeast-3', 'ap-northeast-2',
                        'ap-southeast-1', 'ap-southeast-2', 'ap-northeast-1',
                        'ca-central-1', 'eu-central-1', 'eu-west-1', 'eu-west-2',
                        'eu-south-1', 'eu-west-3', 'eu-north-1', 'me-south-1', 'sa-east-1']
        
    return regions_name



def get_credentials_assume_role(account_id, role_name="Cloudteam-FinOps", external_id=None):
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


def get_aws_session(account_id,region=None,role_name="Cloudteam-FinOps"):
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
    
# async def async_get_aws_session(account_id, region=None, role_name='Cloudteam-FinOps'):
#     credentials =  get_credentials_assume_role(account_id, role_name, region)
#     if credentials:
#         return [
#             aioboto3.Session(
#             aws_access_key_id=credentials['AccessKeyId'],
#             aws_secret_access_key=credentials['SecretAccessKey'],
#             aws_session_token=credentials['SessionToken'],
#             region_name=region
#         ),
#         boto3.Session(
#             aws_access_key_id=credentials['AccessKeyId'],
#             aws_secret_access_key=credentials['SecretAccessKey'],
#             aws_session_token=credentials['SessionToken'],
#             region_name=region
#         )
#         ]
#     return None




   