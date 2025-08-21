from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_application_autoscaling(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function retrieves and saves information about scalable targets for different service
    namespaces using the Application Auto Scaling client.

    :param file_path: The `file_path` parameter in the `list_application_autoscaling` function is the
    path where the output file will be saved. It should be a string representing the file path where the
    inventory data will be stored
    :param session: The `session` parameter in the `list_application_autoscaling` function is typically
    an instance of a boto3 session that is used to create a client for interacting with AWS services. It
    allows you to configure credentials, region, and other settings for making API calls to AWS
    services. You can create
    :param region: Region is a string representing the geographical region where the resources are
    located. It is used to specify the AWS region for the application autoscaling client
    :param time_generated: The `time_generated` parameter in the `list_application_autoscaling` function
    represents the timestamp or time at which the autoscaling inventory data is being generated or
    collected. This timestamp is typically used to track when the data was retrieved or processed. It
    can be in a specific format like ISO 860
    :param account: The `account` parameter in the `list_application_autoscaling` function seems to be a
    dictionary containing information about the account. It likely includes the account ID and account
    name
    """
    idx = 0
    servicesNamespaces = ['ecs', 'elasticmapreduce', 'ec2', 'appstream', 'dynamodb', 'rds', 'sagemaker',
                          'custom-resource', 'comprehend', 'lambda', 'cassandra', 'kafka', 'elasticache', 'neptune']
    client = session.client('application-autoscaling',
                            region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    inventory = []
    for namespaces in servicesNamespaces:
        next_token = None
        while True:
            try:
                inventory = []
                response = client.describe_scalable_targets(
                    ServiceNamespace=namespaces, NextToken=next_token) if next_token else client.describe_scalable_targets(
                    ServiceNamespace=namespaces)
                for resource in response.get('ScalableTargets', []):
                    arn = resource.get('ScalableTargetARN', '')
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
