from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_applicationcostprofiler(file_path, session, region, time_generated, account):
    """
    This Python function retrieves and saves application cost profiler report definitions for a
    specified account and region.

    :param file_path: The `file_path` parameter in the `list_applicationcostprofiler` function is the
    path where the inventory data will be saved as a file. It should be a string representing the file
    path where you want to save the data. For example, it could be something like "/path/to/save/in
    :param session: The `session` parameter is an AWS session object that is used to create a client for
    the Application Cost Profiler service in a specific region. It allows the function to interact with
    the service using the credentials and configuration provided in the session object
    :param region: The `region` parameter in the `list_applicationcostprofiler` function refers to the
    AWS region where the Application Cost Profiler service will be accessed. This parameter specifies
    the geographical area in which the resources will be managed and where the cost profiling reports
    will be generated. It is used to configure the
    :param time_generated: Time_generated is a timestamp indicating when the report is generated. It is
    used in the function to capture the time at which the report is being generated for tracking and
    identification purposes
    :param account: The `account` parameter in the `list_applicationcostprofiler` function seems to be a
    dictionary containing information about an AWS account. It likely includes the account ID and
    account name
    """
    next_token = None
    idx = 0
    client = session.client('application-cost-profiler', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_report_definitions(
                nextToken=next_token) if next_token else client.list_report_definitions()
            for resource in response.get('reportDefinitions', []):
                report = resource.get('reportId', '')
                arn = f"arn:aws:application-cost-profiler:{region}:{account_id}:report/{report}"
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
