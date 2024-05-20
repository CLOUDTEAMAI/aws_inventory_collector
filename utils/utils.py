import json
from os import path, mkdir
from datetime import datetime, timedelta
import pandas as pd


def create_folder_if_not_exist(list_dir_path):
    """
    The function `create_folder_if_not_exist` checks if a list of directories exist and creates them if
    they do not.

    :param list_dir_path: A list of directory paths for which you want to create folders if they do not
    already exist
    """
    for i in list_dir_path:
        if not path.exists(i):
            mkdir(i)


def chunk_list(data, chunk_size=500):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]


def extract_common_info(arn, resource, region, account_id, timegenerated, account_name=""):
    """
    The function `extract_common_info` takes specific parameters and returns a dictionary containing
    common information related to a resource.

    :param arn: An Amazon Resource Name (ARN) is a unique identifier assigned to resources in AWS. It
    includes information about the AWS region, account ID, resource type, and a unique identifier for
    the resource
    :param resource: Resource refers to the specific information or data related to a particular AWS
    resource that you want to extract common information from. This could include details such as
    resource type, configuration settings, tags, etc
    :param region: Region refers to the geographical region where the AWS resource is located. It is a
    key parameter used in AWS services to specify the location of the resource
    :param account_id: The `account_id` parameter in the `extract_common_info` function represents the
    unique identifier of the AWS account associated with the resource being processed
    :param timegenerated: The `timegenerated` parameter in the `extract_common_info` function represents
    the time when the information was generated or captured. If a specific time is not provided for this
    parameter, the function will default to the current time using `datetime.now().strftime("%Y-%m-%d
    %H:%M
    :param account_name: The `extract_common_info` function takes in several parameters to extract
    common information. Here's a brief description of each parameter:
    :return: The function `extract_common_info` is returning a dictionary with the following keys and
    values:
    - "arn": the value of the `arn` parameter
    - "account_id": the value of the `account_id` parameter
    - "account_name": the value of the `account_name` parameter
    - "region": the value of the `region` parameter
    - "properties": the value
    """
    return {
        "arn": arn,
        "account_id": account_id,
        "account_name": account_name,
        "region": region,
        "properties": resource,
        "timegenerated": timegenerated or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }


def extract_common_info_metrics(account_id, id, resource, label, timegenerated, account_name):
    """
    The function `extract_common_info_metrics` extracts common information metrics including account ID,
    resource properties, label, time generated, and account name.

    :param account_id: The `account_id` parameter in the `extract_common_info_metrics` function
    represents the unique identifier of the account to which the metrics belong. It is used to associate
    the metrics data with a specific account in the system
    :param id: The `id` parameter in the `extract_common_info_metrics` function represents the unique
    identifier associated with the resource or data being processed. It is used to uniquely identify the
    data or resource within the system
    :param resource: Resource typically refers to the data or information related to a specific entity
    or object. In the context of the function `extract_common_info_metrics`, the `resource` parameter is
    likely a collection of properties or attributes associated with a particular metric or data point.
    This could include details such as values, types,
    :param label: The `label` parameter in the `extract_common_info_metrics` function represents a
    descriptive label associated with the resource being processed. It is included in the returned
    dictionary along with other common information metrics such as `id`, `account_id`, `account_name`,
    `properties`, and `timegenerated`
    :param timegenerated: The `timegenerated` parameter in the `extract_common_info_metrics` function is
    a timestamp indicating when the information or metrics were generated. If a value is not provided
    for `timegenerated`, the function will default to the current date and time using
    `datetime.now().strftime("%Y-%m-%d
    :param account_name: The `account_name` parameter in the `extract_common_info_metrics` function
    represents the name of the account associated with the metrics being extracted. It is one of the
    input parameters required by the function to create a dictionary containing common information
    metrics
    :return: The function `extract_common_info_metrics` is returning a dictionary containing the
    following key-value pairs:
    - 'id': id
    - 'account_id': account_id
    - 'account_name': account_name
    - 'label': label
    - 'properties': resource
    - 'timegenerated': timegenerated or the current datetime in the format "%Y-%m-%d %H:%M:%S"
    """
    return {
        'id': id,
        'account_id': account_id,
        'account_name': account_name,
        'label': label,
        'properties': resource,
        'timegenerated': timegenerated or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }


def datetime_converter(o):
    """
    The function `datetime_converter` attempts to convert a datetime object to a string representation.

    :param o: The `o` parameter in the `datetime_converter` function is used to represent the object
    that needs to be converted to a string representation. The function attempts to convert the object
    `o` to a string if it is an instance of the `datetime` class
    :return: If the input `o` is an instance of the `datetime` class, then the `__str__()`
    representation of that datetime object is being returned.
    """
    try:
        if isinstance(o, datetime):
            return str(o)
    except Exception as ex:
        print(f"datetime_convertor function faild \n {ex}")


def save_as_file_parquet(inventory, file_path, file_name):
    """
    The function `save_as_file_parquet` takes an inventory, file path, and file name as input, converts
    the inventory data into a DataFrame, ensures consistent data types for Parquet compatibility, and
    saves the DataFrame to a Parquet file at the specified path.

    :param inventory: Inventory is a list of dictionaries containing data that you want to save to a
    Parquet file. Each dictionary represents a row of data with keys as column names and values as the
    corresponding data for that row
    :param file_path: The `file_path` parameter in the `save_as_file_parquet` function refers to the
    directory path where you want to save the Parquet file. It should be a string representing the
    directory path where the file will be saved. For example, it could be something like
    "/path/to/directory
    :param file_name: The `file_name` parameter is a string that represents the name of the file you
    want to save the inventory data as in Parquet format. It should include the file extension
    ".parquet" at the end to indicate that it is a Parquet file
    """
    try:
        if inventory:
            df = pd.DataFrame(inventory)
            file_path = path.join(file_path, file_name)
            # Ensure 'properties' is a string (JSON), as Parquet requires consistent data types
            df['properties'] = df['properties'].apply(lambda x: json.dumps(
                x, default=datetime_converter) if not isinstance(x, str) else x)
            # Save the DataFrame to a Parquet file
            df.to_parquet(file_path, index=False)
    except Exception as ex:
        print(f"save_as_file_parquet function faild \n{ex}")


def save_as_file_parquet_metrics(metrics, file_path, file_name):
    try:
        if metrics:
            df = pd.DataFrame(metrics)
            file_path = path.join(file_path, file_name)
            df.to_parquet(file_path, index=False)
    except Exception as ex:
        print(f"save_as_file_parquet_metrics function faild \n{ex}")


def get_script_name_without_extension(script_path):
    """
    Extracts the script name without the .py extension from the full path.
    """
    return path.splitext(path.basename(script_path))[0]


def generate_parquet_prefix(script_path, region, account_id, idx):
    """
    Generates a prefix for a Parquet file including the script name, region, and account_id.
    """
    script_name = get_script_name_without_extension(script_path)
    return f'{script_name}-{region}-{account_id}-{idx}.parquet'


def cw_build_metrics_queries(resource_ids, namespace, metric_name, dimensions_name, dimensions, statistics, granularity):
    query_list = []
    for i, resource_id in enumerate([f'{resource_id}@{stat}' for stat in statistics for resource_id in resource_ids], start=1):
        dimensions_template = [
            {
                "Name": dimensions_name,
                "Value": resource_id.split("@")[0]
            }
        ]
        for dimension in dimensions:
            dimensions_template.append(dimension)
        query_list.append(
            {
                'Id': f'a{i}',
                'MetricStat': {
                    'Metric': {
                        'Namespace': namespace,
                        'MetricName': metric_name,
                        'Dimensions': dimensions_template
                    },
                    'Period': granularity,
                    'Stat': resource_id.split("@")[1]
                },
                'ReturnData': True
            }
        )
    return query_list


def get_resource_utilization_metric(session, region, inventory, account, metrics, timegenerated):
    client = session.client('cloudwatch', region_name=region)
    account_id = account['account_id']
    end_time = datetime.utcnow()
    query = []
    resource_metrics_list = []
    for metric in metrics:
        start_time = end_time - timedelta(metric['days_ago'])
        query = cw_build_metrics_queries(inventory, metric['aws_namespace'],
                                         metric['aws_metric_name'], metric['aws_dimensions_name'], metric.get('aws_dimensions', []), metric['aws_statistics'], metric['granularity_seconds'])
        response = client.get_metric_data(
            MetricDataQueries=query,
            StartTime=start_time,
            EndTime=end_time
        )
        for result in response.get('MetricDataResults', []):
            for timestamp, value in zip(result['Timestamps'], result['Values']):
                resource_metrics_list.append(
                    {
                        'account_id': account_id,
                        'region': region,
                        'timegenerated': timegenerated,
                        'resource_id': result['Label'].split(' ')[0],
                        'namespace': metric["aws_namespace"],
                        'metric': metric['aws_metric_name'],
                        'instance': '',
                        'unit': metric['aws_unit'],
                        'aggregation': result['Label'].split(' ')[1],
                        'value': value,
                        'timestamp': timestamp.isoformat(),
                        'granularity_sec': metric['granularity_seconds'],
                        'days_ago': metric['days_ago']
                    }
                )
    return resource_metrics_list
