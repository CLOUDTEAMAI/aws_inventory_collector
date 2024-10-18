from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def sagemaker_endpoint_metrics(file_path, session, region, account, metrics, time_generated):
    """
    This Python function retrieves metrics for SageMaker endpoints and saves them to a file in Parquet
    format.

    :param file_path: The `file_path` parameter in the `sagemaker_endpoint_metrics` function is the path
    where the metrics data will be saved as a file. It is the location where the Parquet file containing
    the metrics will be stored
    :param session: The `session` parameter in the `sagemaker_endpoint_metrics` function is typically an
    instance of a boto3 session that is used to create a client for interacting with AWS services. It
    allows you to make API calls to Amazon SageMaker in this case
    :param region: The `region` parameter in the `sagemaker_endpoint_metrics` function is used to
    specify the AWS region where the Amazon SageMaker endpoints are located. SageMaker endpoints are
    resources that host machine learning models for inference. The region parameter is important for
    specifying the geographical location where the SageMaker endpoints are
    :param account: The `account` parameter in the `sagemaker_endpoint_metrics` function seems to be a
    dictionary containing information about the account. It likely includes the account ID and possibly
    other account-related details needed for interacting with AWS services using the provided `session`
    :param metrics: The `metrics` parameter in the `sagemaker_endpoint_metrics` function is used to
    store the metrics related to SageMaker endpoints. These metrics could include resource utilization
    metrics, performance metrics, or any other relevant data that you want to track for the SageMaker
    endpoints. The function seems to be iterating
    :param time_generated: The `time_generated` parameter in the `sagemaker_endpoint_metrics` function
    is used to specify the time at which the metrics are generated or collected. This parameter is
    likely a timestamp or datetime value that indicates when the metrics data was gathered for the
    SageMaker endpoints. It helps in tracking and organizing
    """
    next_token = None
    idx = 0
    client = session.client('sagemaker', region_name=region)
    account_id = account['account_id']
    while True:
        try:
            clusters_idx = 0
            inventory = []
            addons = {"type": "sagemaker_endpoint"}
            addons['nodes'] = []
            response = client.list_endpoints(
                NextToken=next_token) if next_token else client.list_endpoints()
            for resource in response.get('Endpoints', []):
                inventory.append(resource['EndpointName'])
                addons['nodes'].append(
                    {"EndpointName": resource['EndpointName'], "nodes": []})
                addons['nodes'][clusters_idx]['nodes'].append('AllTraffic')
                for variant in resource.get('ProductionVariants', []):
                    addons['nodes'][clusters_idx]['nodes'].append(
                        variant['name'])
                clusters_idx = clusters_idx + 1
            if inventory:
                metrics = get_resource_utilization_metric(
                    session, region, inventory, account, metrics, time_generated, addons)
                save_as_file_parquet_metrics(metrics, file_path, generate_parquet_prefix(
                    str(stack()[0][3]), region, f'{account_id}-metrics', idx))
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
