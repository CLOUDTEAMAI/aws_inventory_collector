from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_ecr_repositories(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function lists ECR repositories, extracts common information, and saves the data as
    Parquet files.

    :param file_path: The `file_path` parameter in the `list_ecr_repositories` function is the path
    where the output files will be saved. It should be a string representing the directory or file path
    where the Parquet files will be stored
    :param session: The `session` parameter in the `list_ecr_repositories` function is typically an
    instance of a boto3 session that allows you to create service clients for AWS services. It is used
    to create a client for the Amazon Elastic Container Registry (ECR) service in the specified AWS
    region. This
    :param region: The `region` parameter in the `list_ecr_repositories` function refers to the AWS
    region where the Amazon ECR (Elastic Container Registry) repositories are located. This parameter is
    used to specify the region when creating an ECR client in the AWS SDK session. It is important to
    provide
    :param time_generated: The `time_generated` parameter in the `list_ecr_repositories` function is
    used to specify the timestamp or time at which the inventory of ECR repositories is generated. This
    timestamp is typically used for tracking and auditing purposes to know when the inventory data was
    collected
    :param account: The `account` parameter seems to be a dictionary containing information about an AWS
    account. It likely includes keys such as 'account_id' and 'account_name'. In the provided code
    snippet, the account information is used to extract the account ID and account name for further
    processing
    """
    next_token = None
    idx = 0
    client = session.client('ecr', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_repositories(
                nextToken=next_token) if next_token else client.describe_repositories()
            for repo in response.get('repositories', []):
                if 'createdAt' in repo:
                    repo['createdAt'] = repo['createdAt'].isoformat()
                repo_arn = repo['repositoryArn']
                inventory_object = extract_common_info(
                    repo_arn, repo, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('nextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def list_ecr_repositories_images(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function retrieves information about ECR repositories and their images, then saves the
    data as Parquet files.

    :param file_path: The `file_path` parameter is the file path where the output data will be saved. It
    is the location where the Parquet files containing the inventory information will be stored
    :param session: The `session` parameter in the `list_ecr_repositories_images` function is typically
    an instance of a boto3 session that allows you to create service clients for AWS services. It is
    used to create a client for the Amazon Elastic Container Registry (ECR) service in the specified
    region. This
    :param region: The `region` parameter in the `list_ecr_repositories_images` function is used to
    specify the AWS region where the Amazon ECR (Elastic Container Registry) resources are located. This
    parameter is required to create a client for the ECR service in the specified region and to retrieve
    information about
    :param time_generated: The `time_generated` parameter in the `list_ecr_repositories_images` function
    is used to specify the timestamp or datetime when the inventory of ECR repositories and images is
    being generated. This parameter is likely used to track when the data was collected or to include a
    timestamp in the output file names
    :param account: The `account` parameter seems to be a dictionary containing information about an AWS
    account. It likely includes keys such as 'account_id' and 'account_name'. This information is used
    within the `list_ecr_repositories_images` function to retrieve and process data related to ECR
    repositories within that AWS
    """
    next_token = None
    idx = 0
    client = session.client('ecr', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    repositories = []
    while True:
        try:
            response = client.describe_repositories(
                nextToken=next_token) if next_token else client.describe_repositories()
            for resource in response.get('repositories', []):
                repositories.append(
                    {'name': resource['repositoryName'], 'arn': resource['repositoryArn']})
            next_token = response.get('nextToken', None)
            if not next_token:
                break
        except Exception as e:
            print(e)
            break

    for repo in repositories:
        while True:
            try:
                inventory = []
                response = client.describe_images(
                    nextToken=next_token, repositoryName=repo['name']) if next_token else client.describe_images(repositoryName=repo['name'])
                for resource in response.get('imageDetails', []):
                    if 'imagePushedAt' in resource:
                        resource['imagePushedAt'] = resource['imagePushedAt'].isoformat()
                    if 'lastRecordedPullTime' in resource:
                        resource['lastRecordedPullTime'] = resource['lastRecordedPullTime'].isoformat(
                        )
                    if 'imageScanCompletedAt' in resource.get('imageScanFindingsSummary', {}):
                        resource['imageScanFindingsSummary']['imageScanCompletedAt'] = resource[
                            'imageScanFindingsSummary']['imageScanCompletedAt'].isoformat()
                    if 'vulnerabilitySourceUpdatedAt' in resource.get('imageScanFindingsSummary', {}):
                        resource['imageScanFindingsSummary']['vulnerabilitySourceUpdatedAt'] = resource[
                            'imageScanFindingsSummary']['vulnerabilitySourceUpdatedAt'].isoformat()

                    inventory_object = extract_common_info(
                        repo['arn'], resource, region, account_id, time_generated, account_name)
                    inventory.append(inventory_object)
                save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                    str(stack()[0][3]), region, account_id, idx))
                next_token = response.get('nextToken', None)
                idx = idx + 1
                if not next_token:
                    break
            except Exception as e:
                print(e)
                break
