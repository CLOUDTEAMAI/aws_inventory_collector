from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix
# https://awslabs.github.io/aws-crt-python/api/http.html#awscrt.http.HttpRequest
from awscrt.http import HttpRequest
# https://awslabs.github.io/aws-crt-python/api/auth.html
from awscrt.auth import AwsCredentialsProvider, AwsSignatureType, AwsSigningAlgorithm, AwsSigningConfig, aws_sign_request
import requests


def list_account_support(file_path, session, region, time_generated, account):
    """
    The function `list_account_support` makes a GET request to retrieve support plan information for a
    specific AWS account and saves the response as a Parquet file.

    :param file_path: The `file_path` parameter is a string representing the file path where the output
    will be saved. It is the location where the function will save the data retrieved for the specified
    account
    :param session: The `session` parameter in the `list_account_support` function is typically an
    instance of a session object that allows you to interact with AWS services using the AWS SDK for
    Python (Boto3). This session object stores configuration information such as credentials, region,
    and other settings needed to make API requests
    :param region: Region is a string representing the AWS region where the account is located. It is
    used in the function to specify the region for making API requests and generating ARN (Amazon
    Resource Name)
    :param time_generated: `time_generated` is a timestamp indicating when the account support
    information is being generated. It is used in the function `list_account_support` to help identify
    when the support information was retrieved for a specific account
    :param account: The `account` parameter in the `list_account_support` function is a dictionary
    containing information about an AWS account. It likely includes keys such as 'account_id' and
    'account_name' to identify the account. The function uses this information to make API requests and
    extract support plan details for the specified
    """
    account_id = account['account_id']
    account_name = str(account.get('account_name')).replace(" ", "_")
    try:
        http_request_obj = HttpRequest(method="GET", path="/v1/getSupportPlan")
        http_request_obj.headers.add(
            "Host", "service.supportplans.us-east-2.api.aws")
        # Create a credentials provider with the AssumeRole provider
        credentials = session.get_credentials().get_frozen_credentials()
        credentials_provider = AwsCredentialsProvider.new_static(
            access_key_id=credentials.access_key,
            secret_access_key=credentials.secret_key,
            session_token=credentials.token
        )
        http_request = aws_sign_request(
            http_request=http_request_obj,
            signing_config=AwsSigningConfig(
                algorithm=AwsSigningAlgorithm.V4,
                signature_type=AwsSignatureType.HTTP_REQUEST_HEADERS,
                credentials_provider=credentials_provider,
                service="supportplans",
                region="us-east-2",
            )
        ).result()
        response = requests.get(
            url="https://service.supportplans.us-east-2.api.aws/v1/getSupportPlan",
            headers=dict(http_request.headers),
            timeout=300
        )
        arn = f'arn:aws:supportplans::{account_id}:support/{account_id}'
        if response:
            client_object = extract_common_info(
                arn, response.json()['supportPlan'], region, account_id, time_generated, account_name)
            save_as_file_parquet([client_object], file_path, generate_parquet_prefix(
                str(stack()[0][3]), 'global', account_id, 0))
    except Exception as e:
        print(e)
