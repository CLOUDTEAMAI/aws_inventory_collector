from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_batch_compute(file_path, session, region, time_generated, account):
    """
    This Python function iterates through compute environments using the AWS Batch client, extracts
    common information, and saves the data as a Parquet file.

    :param file_path: The `file_path` parameter is the path where the output files will be saved. It is
    a string that represents the directory or file path where the results of the batch compute operation
    will be stored
    :param session: The `session` parameter in the `list_batch_compute` function is typically an
    instance of a boto3 session that allows you to create service clients for AWS services. It is used
    to create a client for the AWS Batch service in the specified region
    :param region: Region is a string representing the AWS region where the compute environments are
    located. It is used to specify the region when creating a client for the AWS Batch service
    :param time_generated: Time_generated is a timestamp indicating when the computation was generated.
    It is used in the function to track the time at which the inventory information was collected
    :param account: The `account` parameter in the `list_batch_compute` function seems to be a
    dictionary containing information about an account. It likely includes keys such as 'account_id' and
    'account_name'. This information is used within the function to extract specific details and perform
    operations related to the account being processed
    """
    next_token = None
    idx = 0
    client = session.client('batch', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_compute_environments(
                nextToken=next_token) if next_token else client.describe_compute_environments()
            for resource in response.get('computeEnvironments', []):
                arn = resource.get('computeEnvironmentArn', '')
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


def list_batch_jobs(file_path, session, region, time_generated, account):
    """
    The function `list_batch_jobs` retrieves information about job queues and jobs from AWS Batch and
    saves the data as Parquet files.

    :param file_path: The `file_path` parameter in the `list_batch_jobs` function is the path where the
    output file will be saved. It is the location where the Parquet file containing the job inventory
    information will be stored
    :param session: The `session` parameter in the `list_batch_jobs` function is typically an instance
    of `boto3.Session` that is used to create a client for AWS Batch service. It allows you to make API
    calls to AWS Batch using the specified region. You can create a session like this:
    :param region: The `region` parameter in the `list_batch_jobs` function is used to specify the AWS
    region where the Batch service is located. This parameter is required to create a client for the
    Batch service in the specified region and to perform operations related to Batch jobs within that
    region
    :param time_generated: Time_generated is a parameter that represents the timestamp or time at which
    the batch jobs were generated or executed. It is used in the function `list_batch_jobs` to capture
    the time information related to the batch jobs being processed
    :param account: The `list_batch_jobs` function takes several parameters:
    """
    next_token = None
    idx = 0
    client = session.client('batch', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    job_queues = []
    while True:
        try:
            response = client.describe_job_queues(
                nextToken=next_token, maxResults=100) if next_token else client.list_jobs(maxResults=100)
            for queue in response.get('jobQueues', []):
                job_queues.append(queue['jobQueueName'])
            next_token = response.get('nextToken', None)
            if not next_token:
                break
        except Exception as e:
            print(e)
            break

    for queue in job_queues:
        while True:
            try:
                jobs = []
                inventory = []
                response = client.list_jobs(
                    nextToken=next_token, maxResults=100, jobQueue=queue) if next_token else client.list_jobs(maxResults=100, jobQueue=queue)
                for resource in response.get('jobSummaryList', []):
                    jobs.append(resource.get('jobId'))

                jobs_response = client.describe_jobs(jobs=jobs)
                for job in jobs_response.get('jobs'):
                    arn = job.get('jobArn', '')
                    inventory_object = extract_common_info(
                        arn, job, region, account_id, time_generated, account_name)
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
