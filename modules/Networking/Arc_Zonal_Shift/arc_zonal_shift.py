from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_arc_zonal_shift(file_path, session, region, time_generated, account):
    """
    The function `list_arc_zonal_shift` retrieves and processes inventory data related to arc and zonal
    shifts for a specified account in a given region.

    :param file_path: The `file_path` parameter in the `list_arc_zonal_shift` function is the path where
    the output file will be saved. It should be a string representing the file path where the function
    will save the results of the operation
    :param session: The `session` parameter in the `list_arc_zonal_shift` function is typically an
    instance of a session object that allows you to interact with AWS services. It is used to create a
    client for the `arc-zonal-shift` service in the specified region
    :param region: Region refers to the geographical location where the resources are located or where
    the operations will be performed. It is typically a string representing a specific region such as
    'us-east-1' for US East (N. Virginia) in AWS or 'europe-west1' for Western Europe in Google Cloud
    Platform
    :param time_generated: Time_generated is a timestamp indicating when the data was generated or
    collected. It is typically in a specific format such as ISO 8601 (e.g., '2022-01-31T15:00:00Z').
    This parameter helps in tracking the timing of the data for reference and analysis
    :param account: The `account` parameter is a dictionary containing information about an account. It
    typically includes keys such as 'account_id' and 'account_name'. In the provided code snippet, the
    `account` dictionary is used to extract the 'account_id' and 'account_name' values for further
    processing within the
    """
    next_token = None
    idx = 0
    client = session.client('arc-zonal-shift', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_managed_resources(
                nextToken=next_token) if next_token else client.list_managed_resources()
            for resource in response.get('items', []):
                for arc in resource.get('autoshifts', []):
                    arc['startTime'] = arc['startTime'].isformat()

                for zonal in resource.get('zonalShifts', []):
                    zonal['expiryTime'] = zonal['expiryTime'].isformat()
                    zonal['startTime'] = zonal['startTime'].isformat()

                query_details = client.get_named_query(NamedQueryId=resource)
                arn = resource['arn']
                inventory_object = extract_common_info(
                    arn, query_details, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path,
                                 generate_parquet_prefix(str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('nextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
