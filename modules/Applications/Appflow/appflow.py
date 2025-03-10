from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_appflow(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function retrieves and processes information about appflows, saving the data in Parquet
    format.

    :param file_path: The `file_path` parameter in the `list_appflow` function refers to the path where
    the output file will be saved. This should be a valid file path on your system where you want to
    store the data retrieved by the function
    :param session: The `session` parameter in the `list_appflow` function is an object representing the
    AWS session that is used to create a client for the AWS AppFlow service in a specific region. This
    session object is typically created using the `boto3.Session` class and is used to interact with
    various
    :param region: Region is a string representing the geographic region where the resources are
    located. It is used to specify the AWS region for the AppFlow client
    :param time_generated: The `time_generated` parameter in the `list_appflow` function is used to
    specify the timestamp or time at which the data is generated or collected. This parameter is
    important for tracking when the data was retrieved or processed in the context of the AppFlow flows
    being listed and extracted. It helps in
    :param account: The `account` parameter in the `list_appflow` function seems to be a dictionary
    containing information about an account. It likely includes keys such as 'account_id' and
    'account_name'. This information is used within the function to extract common information and
    generate a file in parquet format
    """
    next_token = None
    idx = 0
    client = session.client('appflow', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_flows(
                nextToken=next_token) if next_token else client.list_flows()
            for resource in response.get('flows', []):
                resource_response = client.list_flows(
                    flowName=resource['flowName'])
                arn = resource_response['flowArn']
                if 'createdAt' in resource_response:
                    resource_response['createdAt'] = resource_response['createdAt'].isoformat(
                    )
                if 'lastUpdatedAt' in resource_response:
                    resource_response['lastUpdatedAt'] = resource_response['lastUpdatedAt'].isoformat(
                    )
                if 'lastRunExecutionDetails' in resource_response:
                    if 'mostRecentExecutionTime' in resource_response.get('lastRunExecutionDetails', {}):
                        resource_response['lastRunExecutionDetails']['mostRecentExecutionTime'] = resource_response[
                            'lastRunExecutionDetails']['mostRecentExecutionTime'].isoformat()
                if 'triggerConfig' in resource_response:
                    if 'triggerProperties' in resource_response.get('triggerConfig', {}):
                        if 'scheduleStartTime' in resource_response.get('triggerConfig', {}).get('triggerProperties', {}):
                            resource_response['triggerConfig']['triggerProperties']['scheduleStartTime'] = resource_response[
                                'triggerConfig']['triggerProperties']['scheduleStartTime'].isoformat()
                        if 'scheduleEndTime' in resource_response.get('triggerConfig', {}).get('triggerProperties', {}):
                            resource_response['triggerConfig']['triggerProperties']['scheduleEndTime'] = resource_response[
                                'triggerConfig']['triggerProperties']['scheduleEndTime'].isoformat()
                        if 'firstExecutionFrom' in resource_response.get('triggerConfig', {}).get('triggerProperties', {}):
                            resource_response['triggerConfig']['triggerProperties']['firstExecutionFrom'] = resource_response[
                                'triggerConfig']['triggerProperties']['firstExecutionFrom'].isoformat()
                object_client = extract_common_info(
                    arn, resource_response, region, account_id, time_generated, account_name)
                inventory.append(object_client)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('nextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
