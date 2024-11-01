from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix
# https://awslabs.github.io/aws-crt-python/api/http.html#awscrt.http.HttpRequest
from awscrt.http import HttpRequest
# https://awslabs.github.io/aws-crt-python/api/auth.html
from awscrt.auth import AwsCredentialsProvider, AwsSignatureType, AwsSigningAlgorithm, AwsSigningConfig, aws_sign_request
import requests


def list_account_support(file_path, session, region, time_generated, account):
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
