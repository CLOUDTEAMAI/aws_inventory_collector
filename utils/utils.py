import json
from os import path, mkdir
from datetime import datetime, timedelta
from pandas import DataFrame


def remove_duplicates(dicts):
    seen = set()
    unique_dicts = []
    for d in dicts:
        # Convert dictionary to a sorted tuple of items
        items_tuple = tuple(sorted(d.items()))
        if items_tuple not in seen:
            seen.add(items_tuple)
            unique_dicts.append(d)
    return unique_dicts


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


def chunk_list(data, chunk_size=499):
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
            df = DataFrame(inventory)
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
            df = DataFrame(metrics)
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
    query_idx = {}
    dimensions_addons = []
    for dimension in dimensions:
        for key, value in dimension.items():
            dimensions_addons.append({"Name": key, "Value": value})
    for i, resource_id in enumerate([f'{resource_id}@{stat}' for stat in statistics for resource_id in resource_ids], start=1):
        dimensions_template = [
            {
                "Name": dimensions_name,
                "Value": resource_id.split("@")[0]
            }
        ]
        dimenstions_ready = dimensions_template + dimensions_addons
        content = {
            'Id': f'a{i}',
            'MetricStat': {
                'Metric': {
                    'Namespace': namespace,
                    'MetricName': metric_name,
                    'Dimensions': dimenstions_ready
                },
                'Period': granularity,
                'Stat': resource_id.split("@")[1]
            },
            'ReturnData': True
        }
        query_idx[f'a{i}'] = content
        query_list.append(content)
    return query_list, query_idx


def cw_build_metrics_queries_custom(resource_ids, namespace, metric_name, dimensions_name, dimensions, statistics, granularity, custom_type_value, addons):
    query_list = []
    enum_list = []
    query_idx = {}
    dimensions_addons = []
    for dimension in dimensions:
        for key, value in dimension.items():
            dimensions_addons.append({"Name": key, "Value": value})
    if addons['type'] in custom_type_value.keys():
        for cluster in addons['nodes']:
            for node in cluster['nodes']:
                for stat in statistics:
                    line = f"{cluster[custom_type_value[addons['type']]['parent']]}@{stat}@{node}"
                    for key, value in cluster.get('items', {}).get(node, {}).items():
                        line = line + f"@{value}"
                    enum_list.append(line)

    for i, resource_id in enumerate(enum_list, start=1):
        items_addons = []
        resource_id_splitted_array = resource_id.split("@")
        dimensions_template = [
            {
                "Name": dimensions_name,
                "Value": resource_id_splitted_array[0] if dimensions_name != custom_type_value[addons['type']]['comparison_value'] else resource_id_splitted_array[2]
            }
        ]
        if resource_id_splitted_array[2]:
            nodes_addons = [
                {"Name": custom_type_value[addons['type']]['comparison_value'], "Value": resource_id_splitted_array[2]}]
        if len(resource_id_splitted_array) > 3:
            for cluster in addons['nodes']:
                for key, value in cluster.get('items', {}).get(resource_id_splitted_array[2], {}).items():
                    items_addons.append({"Name": key, "Value": value})
        dimenstions_ready = remove_duplicates((dimensions_template +
                                              dimensions_addons + nodes_addons + items_addons))
        content = {
            'Id': f'a{i}',
            'MetricStat': {
                'Metric': {
                    'Namespace': namespace,
                    'MetricName': metric_name,
                    'Dimensions': dimenstions_ready
                },
                'Period': granularity,
                'Stat': resource_id_splitted_array[1]
            },
            'ReturnData': True
        }
        query_idx[f'a{i}'] = content
        query_list.append(content)
    return query_list, query_idx


def get_resource_utilization_metric(session, region, inventory, account, metrics, timegenerated, addons={}):
    client = session.client('cloudwatch', region_name=region)
    account_id = account['account_id']
    end_time = datetime.utcnow()
    query = []
    resource_metrics_list = []
    custom_type_value = {
        'elasticache': {
            'parent': 'CacheClusterId',
            'comparison_value': 'CacheNodeId'
        },
        'transitgateway': {
            'parent': 'TransitGatewayId',
            'comparison_value': 'TransitGatewayAttachment'
        },
        'cloudhsmv2': {
            'parent': 'ClusterId',
            'comparison_value': 'HsmId'
        },
        "privatelinkendpoints": {
            'parent': 'VpcId',
            'comparison_value': 'VPC Endpoint Id',
            'items': ['Endpoint Type', 'Service Name', 'Vpc Id']
        },
        "elbv2-application": {
            'parent': 'TargetGroupName',
            'comparison_value': 'LoadBalancer'
        },
        "msk-nodes": {
            'parent': 'Cluster Name',
            'comparison_value': 'Broker ID'
        }
    }
    for metric in metrics:
        start_time = end_time - timedelta(metric['days_ago'])
        query, query_idx = cw_build_metrics_queries_custom(inventory, metric['aws_namespace'],
                                                           metric['aws_metric_name'], metric['aws_dimensions_name'], metric.get('aws_dimensions', []), metric['aws_statistics'], metric['granularity_seconds'], custom_type_value, addons) if addons else cw_build_metrics_queries(inventory, metric['aws_namespace'],
                                                                                                                                                                                                                                                                                   metric['aws_metric_name'], metric['aws_dimensions_name'], metric.get('aws_dimensions', []), metric['aws_statistics'], metric['granularity_seconds'])
        for sublist in chunk_list(query):
            response = client.get_metric_data(
                MetricDataQueries=sublist,
                StartTime=start_time,
                EndTime=end_time
            )
            for result in response.get('MetricDataResults', []):
                for timestamp, value in zip(result['Timestamps'], result['Values']):
                    if addons.get('type') in custom_type_value.keys():
                        for dimension in query_idx[result['Id']]['MetricStat']['Metric']['Dimensions']:
                            if custom_type_value[addons['type']].get('comparison_value', None) is not None:
                                comparison_value = custom_type_value[addons['type']
                                                                     ]['comparison_value']
                            # elif childs
                            if dimension['Name'] == comparison_value:
                                instance_value = dimension['Value']
                            else:
                                instance_value = query_idx[result['Id']
                                                           ]['MetricStat']['Metric']['Dimensions'][0]['Value']
                    else:
                        instance_value = ''
                    resource_metrics_list.append(
                        {
                            'account_id': account_id,
                            'region': region,
                            'timegenerated': timegenerated,
                            'resource_id': query_idx[result['Id']]['MetricStat']['Metric']['Dimensions'][0]['Value'],
                            'namespace': metric["aws_namespace"],
                            'metric': metric['aws_metric_name'],
                            'instance': instance_value if instance_value else '',
                            'unit': metric['aws_unit'],
                            'aggregation': query_idx[result['Id']]['MetricStat']['Stat'],
                            'value': value,
                            'timestamp': timestamp.isoformat(),
                            'granularity_sec': metric['granularity_seconds'],
                            'days_ago': metric['days_ago']
                        }
                    )
    return resource_metrics_list
