from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_sagemaker(file_path, session, region, time_generated, account):
    """
    This Python function lists SageMaker clusters, extracts common information, and saves the data as
    Parquet files.

    :param file_path: The `file_path` parameter in the `list_sagemaker` function is the path where the
    inventory data will be saved as a file. It is the location where the Parquet file containing the
    extracted information about SageMaker clusters will be stored
    :param session: The `session` parameter in the `list_sagemaker` function is an object representing
    the current session. It is typically created using the `boto3.Session` class and is used to create
    clients and resources for AWS services
    :param region: The `region` parameter in the `list_sagemaker` function refers to the AWS region
    where the Amazon SageMaker resources are located. This parameter is used to specify the region when
    creating a client for the SageMaker service and when extracting information about SageMaker clusters
    in that region. It is important
    :param time_generated: The `time_generated` parameter in the `list_sagemaker` function is used to
    specify the timestamp or time at which the inventory data is being generated. This timestamp is
    typically used for tracking and auditing purposes to know when the inventory information was
    collected or updated. It is important to provide an accurate
    :param account: The `account` parameter in the `list_sagemaker` function seems to be a dictionary
    containing information about the account. It likely has the following structure:
    """
    next_token = None
    idx = 0
    client = session.client('sagemaker', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_clusters(
                NextToken=next_token) if next_token else client.list_clusters()
            for resource in response.get('ClusterSummaries', []):
                arn = resource['ClusterArn']
                cluster = client.describe_cluster(
                    ClusterName=resource['ClusterName'])
                cluster['CreationTime'] = cluster['CreationTime'].isoformat()
                inventory_object = extract_common_info(
                    arn, cluster, region, account_id, time_generated, account_name)
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


def list_sagemaker_domain(file_path, session, region, time_generated, account):
    """
    This Python function lists SageMaker domains, extracts common information, and saves the data as
    Parquet files.

    :param file_path: The `file_path` parameter in the `list_sagemaker_domain` function is the path
    where the output file will be saved. It should be a string representing the file path where the
    inventory data will be stored
    :param session: The `session` parameter in the `list_sagemaker_domain` function is typically an
    instance of a boto3 session that is used to create a client for interacting with AWS services. It
    allows you to configure credentials, region, and other settings for making API calls to AWS services
    like SageMaker
    :param region: The `region` parameter in the `list_sagemaker_domain` function is used to specify the
    AWS region where the Amazon SageMaker resources are located. SageMaker resources such as domains are
    region-specific, so you need to provide the region name where you want to list the domains. For
    example,
    :param time_generated: The `time_generated` parameter in the `list_sagemaker_domain` function is
    used to specify the timestamp or time at which the inventory data is generated or collected. This
    timestamp is typically used for tracking when the inventory information was retrieved or processed.
    It helps in maintaining a record of when the data
    :param account: The `account` parameter in the `list_sagemaker_domain` function seems to be a
    dictionary containing information about the AWS account. It likely has the following structure:
    """
    next_token = None
    idx = 0
    client = session.client('sagemaker', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        inventory = []
        response = client.list_domains(
            NextToken=next_token) if next_token else client.list_domains()
        for resource in response.get('Domains', []):
            arn = resource['ClusterArn']
            cluster = client.describe_domain(
                DomainId=resource['DomainId'])
            cluster['CreationTime'] = cluster['CreationTime'].isoformat()
            cluster['LastModifiedTime'] = cluster['LastModifiedTime'].isoformat()
            inventory_object = extract_common_info(
                arn, cluster, region, account_id, time_generated, account_name)
            inventory.append(inventory_object)
        save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
            str(stack()[0][3]), region, account_id, idx))
        next_token = response.get('NextToken', None)
        idx = idx + 1
        if not next_token:
            break
