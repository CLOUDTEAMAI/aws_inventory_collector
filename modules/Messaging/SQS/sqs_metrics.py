from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def sqs_metrics(file_path, session, region, account, metrics, time_generated, boto_config):
    next_token = None
    idx = 0
    account_id = account['account_id']
    while True:
        try:
            client = session.client(
                'sqs', region_name=region, config=boto_config)
            inventory = []
            response = client.list_queues(
                NextToken=next_token) if next_token else client.list_queues()
            for resource in response.get('QueueUrls', []):
                attributes = client.get_queue_attributes(
                    QueueUrl=resource, AttributeNames=['All'])['Attributes']
                inventory.append(attributes['QueueArn'].split(':')[-1])
            metrics = get_resource_utilization_metric(
                session, region, inventory, account, metrics, time_generated)
            save_as_file_parquet_metrics(metrics, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, f'{account_id}-metrics', idx))
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
