from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def dx_vif_metrics(file_path, session, region, account, metrics, time_generated):
    """
    The function `dx_vif_metrics` retrieves Direct Connect VIF metrics for a specified account and
    region and saves them to a file in Parquet format.

    :param file_path: The `file_path` parameter is a string that represents the path where the metrics
    data will be saved as a file. It should include the file name and extension (e.g.,
    "metrics_data.csv")
    :param session: The `session` parameter in the `dx_vif_metrics` function is typically an instance of
    a boto3 session that allows you to create service clients for AWS services. It is used to interact
    with the AWS Direct Connect service in this function
    :param region: Region is the geographical area where the AWS resources are located. It is used to
    specify the AWS region in which the Direct Connect operations will be performed. Examples of regions
    include 'us-east-1', 'eu-west-1', 'ap-southeast-2', etc
    :param account: The `account` parameter in the `dx_vif_metrics` function is a dictionary containing
    information about the account. It likely includes details such as the account ID, which can be
    accessed using `account['account_id']`. This information is used within the function to identify the
    account for which the metrics
    :param metrics: Metrics is a list of metrics that you want to collect for the Direct Connect Virtual
    Interface (VIF) resources. These metrics could include information such as bandwidth utilization,
    packet loss, latency, etc. The function `dx_vif_metrics` is designed to retrieve these metrics for
    the specified Direct Connect V
    :param time_generated: The `time_generated` parameter in the `dx_vif_metrics` function represents
    the timestamp or time at which the metrics are generated or collected. This parameter is used to
    track when the metrics data was obtained and can be helpful for analyzing the data over time or for
    ensuring data freshness. It is typically
    """
    next_token = None
    idx = 0
    client = session.client('directconnect', region_name=region)
    account_id = account['account_id']
    while True:
        try:
            clusters_idx = 0
            inventory = []
            addons = {"type": "directconnect_vif"}
            addons['nodes'] = []
            response = client.describe_virtual_interfaces(
                NextToken=next_token) if next_token else client.describe_virtual_interfaces()
            for resource in response.get('virtualInterfaces', []):
                inventory.append(resource["connectionId"])
                addons['nodes'].append(
                    {"ConnectionId": resource["connectionId"], "nodes": []})
                addons['nodes'][clusters_idx]['nodes'].append(
                    resource['virtualInterfaceId'])
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
