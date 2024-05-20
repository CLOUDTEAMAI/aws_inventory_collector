from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_batch_compute(file_path, session, region, time_generated, account):
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
