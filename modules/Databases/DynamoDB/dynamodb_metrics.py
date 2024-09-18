from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def dynamodb_tables_metrics(file_path, session, region, account, metrics, time_generated):
    """
    This function retrieves DynamoDB table names, collects resource utilization metrics for each table,
    and saves the metrics to a Parquet file.

    :param file_path: The `file_path` parameter in the `dynamodb_tables_metrics` function is the path
    where the metrics data will be saved as a file. It is a string that represents the location or
    directory where the metrics data will be stored
    :param session: The `session` parameter in the `dynamodb_tables_metrics` function is typically an
    instance of a boto3 session that is used to create a client for interacting with AWS services. It
    allows you to configure credentials, region, and other settings for making API calls to AWS services
    like DynamoDB
    :param region: The `region` parameter in the `dynamodb_tables_metrics` function is used to specify
    the AWS region where the DynamoDB tables are located. This parameter is important for creating the
    DynamoDB client in the specified region and for listing the tables in that region. It helps in
    ensuring that the function interacts
    :param account: The `account` parameter in the `dynamodb_tables_metrics` function seems to represent
    an account object or dictionary containing information about the AWS account. It likely includes
    details such as the account ID, which can be accessed using `account['account_id']` in the function
    :param metrics: The `metrics` parameter in the `dynamodb_tables_metrics` function likely refers to a
    data structure or object that stores metrics related to DynamoDB tables. These metrics could include
    information such as read/write capacity units, storage usage, or any other performance metrics
    relevant to DynamoDB tables. The function seems
    :param time_generated: The `time_generated` parameter in the `dynamodb_tables_metrics` function is
    used to specify the time at which the metrics are generated or collected. This parameter likely
    represents a timestamp or datetime value indicating when the metrics data was gathered for DynamoDB
    tables
    """
    next_token = None
    idx = 0
    client = session.client('dynamodb', region_name=region)
    account_id = account['account_id']
    while True:
        try:
            inventory = []
            response = client.list_tables(
                ExclusiveStartTableName=next_token) if next_token else client.list_tables()
            if response.get('TableNames', []):
                inventory = response.get('TableNames', [])
            if inventory:
                metrics = get_resource_utilization_metric(
                    session, region, inventory, account, metrics, time_generated)
                save_as_file_parquet_metrics(metrics, file_path, generate_parquet_prefix(
                    str(stack()[0][3]), region, f'{account_id}-metrics', idx))
            next_token = response.get(
                'LastEvaluatedTableName', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
