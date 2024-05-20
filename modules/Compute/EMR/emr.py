from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_emr(file_path, session, region, time_generated, account):
    """
    The function `list_emr` retrieves information about EMR clusters and saves it to a Parquet file.

    :param file_path: The `file_path` parameter in the `list_emr` function is the path where the output
    file will be saved. It should be a string representing the file path where the Parquet file will be
    stored
    :param session: The `session` parameter in the `list_emr` function is typically an AWS session
    object that is used to create clients for various AWS services. It is used to interact with the
    Amazon EMR (Elastic MapReduce) service in this specific function. The session object is usually
    created using the
    :param region: The `region` parameter in the `list_emr` function is used to specify the AWS region
    where the Amazon EMR (Elastic MapReduce) clusters are located. This parameter is required to create
    a client for the EMR service in the specified region and to list the clusters in that region
    :param time_generated: The `time_generated` parameter in the `list_emr` function is used to specify
    the timestamp or datetime when the inventory data is being generated or collected. This information
    is important for tracking when the inventory was gathered and can be used for various purposes such
    as auditing, monitoring, or analysis
    :param account: The `account` parameter in the `list_emr` function seems to be a dictionary
    containing information about the account. It likely includes keys such as 'account_id' and
    'account_name'
    """
    next_token = None
    idx = 0
    client = session.client('emr', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_clusters(
                Marker=next_token) if next_token else client.list_clusters()
            for resource in response.get('Clusters', []):
                if 'Timeline' in resource:
                    if 'CreationDateTime' in resource['Timeline']:
                        resource['Timeline']['CreationDateTime'] = resource['Timeline']['CreationDateTime'].isoformat(
                        )
                    if 'ReadyDateTime' in resource['Timeline']:
                        resource['Timeline']['ReadyDateTime'] = resource['Timeline']['ReadyDateTime'].isoformat(
                        )
                    if 'EndDateTime' in resource['Timeline']:
                        resource['Timeline']['EndDateTime'] = resource['Timeline']['EndDateTime'].isoformat(
                        )

                arn = resource['ClusterArn']
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('Marker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def list_emr_instance_group(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('emr', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    clusters = []
    while True:
        try:
            response = client.list_clusters(
                Marker=next_token) if next_token else client.list_clusters()
            for cluster in response.get('Clusters', []):
                clusters.append(cluster['Id'])
            next_token = response.get('Marker', None)
            if not next_token:
                break
        except Exception as e:
            print(e)
            break

    for cluster in clusters:
        while True:
            try:
                next_token = None
                inventory = []
                response = client.list_instance_groups(
                    Marker=next_token, ClusterId=cluster) if next_token else client.list_instance_groups(ClusterId=cluster)
                for resource in response.get('InstanceGroups', []):
                    if 'Status' in resource:
                        if 'CreationDateTime' in resource['Timeline']:
                            resource['Status']['Timeline']['CreationDateTime'] = resource['Status']['Timeline']['CreationDateTime'].isoformat(
                            )
                        if 'ReadyDateTime' in resource['Timeline']:
                            resource['Status']['Timeline']['ReadyDateTime'] = resource['Status']['Timeline']['ReadyDateTime'].isoformat(
                            )
                        if 'EndDateTime' in resource['Timeline']:
                            resource['Status']['Timeline']['EndDateTime'] = resource['Status']['Timeline']['EndDateTime'].isoformat(
                            )

                    arn = resource['ClusterArn']
                    inventory_object = extract_common_info(
                        arn, resource, region, account_id, time_generated, account_name)
                    inventory.append(inventory_object)
                save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                    str(stack()[0][3]), region, account_id, idx))
                next_token = response.get('Marker', None)
                idx = idx + 1
                if not next_token:
                    break
            except Exception as e:
                print(e)
                break


def list_emr_containers(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('emr-containers', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_virtual_clusters(
                nextToken=next_token) if next_token else client.list_virtual_clusters()
            for resource in response.get('virtualClusters', []):
                if 'createdAt' in resource:
                    resource['createdAt'] = resource['createdAt'].isoformat()

                arn = resource['arn']
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
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


def list_emr_containers_jobs(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('emr-containers', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    clusters = []
    while True:
        try:
            response = client.list_virtual_clusters(
                nextToken=next_token) if next_token else client.list_virtual_clusters()
            for cluster in response.get('virtualClusters', []):
                clusters.append(cluster['id'])
            next_token = response.get('nextToken', None)
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
    for cluster in clusters:
        while True:
            try:
                next_token = None
                inventory = []
                response = client.list_job_runs(
                    Marker=next_token, virtualClusterId=cluster) if next_token else client.list_job_runs(virtualClusterId=cluster)
                for resource in response.get('InstanceGroups', []):
                    if 'createdAt' in resource:
                        resource['createdAt'] = resource['createdAt'].isoformat()
                    if 'finishedAt' in resource:
                        resource['finishedAt'] = resource['finishedAt'].isoformat()

                    arn = resource['arn']
                    inventory_object = extract_common_info(
                        arn, resource, region, account_id, time_generated, account_name)
                    inventory.append(inventory_object)
                save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                    str(stack()[0][3]), region, account_id, idx))
                next_token = response.get('Marker', None)
                idx = idx + 1
                if not next_token:
                    break
            except Exception as e:
                print(e)
                break


def list_emr_serverless_jobs(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('emr-serverless', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    clusters = []
    while True:
        try:
            response = client.list_applications(
                nextToken=next_token) if next_token else client.list_applications()
            for cluster in response.get('applications', []):
                clusters.append(cluster['id'])
            next_token = response.get('nextToken', None)
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
    for cluster in clusters:
        while True:
            try:
                next_token = None
                inventory = []
                response = client.list_job_runs(
                    nextToken=next_token, applicationId=cluster) if next_token else client.list_job_runs(applicationId=cluster)
                for resource in response.get('jobRuns', []):
                    if 'createdAt' in resource:
                        resource['createdAt'] = resource['createdAt'].isoformat()
                    if 'updatedAt' in resource:
                        resource['updatedAt'] = resource['updatedAt'].isoformat()

                    arn = resource['arn']
                    inventory_object = extract_common_info(
                        arn, resource, region, account_id, time_generated, account_name)
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
