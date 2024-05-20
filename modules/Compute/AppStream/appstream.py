from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_appstream(file_path, session, region, time_generated, account):
    """
    The function `list_appstream` retrieves information about image builders in AWS AppStream and saves
    it as a Parquet file.

    :param file_path: The `file_path` parameter is the path where the output file will be saved. It is a
    string that represents the location where the file will be stored
    :param session: The `session` parameter in the `list_appstream` function is typically an instance of
    a boto3 session that allows you to create service clients for AWS services. It is used to create a
    client for the AppStream service in the specified region
    :param region: The `region` parameter in the `list_appstream` function is used to specify the AWS
    region where the AppStream resources are located. This parameter is required to create a client for
    the AppStream service in the specified region and to retrieve information about image builders in
    that region. It helps in ensuring
    :param time_generated: Time when the inventory is generated
    :param account: The `list_appstream` function takes several parameters:
    """
    next_token = None
    idx = 0
    client = session.client('appstream', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_image_builders(
                NextToken=next_token) if next_token else client.describe_image_builders()
            for resource in response.get("ImageBuilders", []):
                image_builder_errors_counter = 0
                arn = resource["Arn"]
                if "CreatedTime" in resource:
                    resource["CreatedTime"] = resource["CreatedTime"].isoformat()
                for cleaner in resource.get("ImageBuilderErrors", []):
                    if 'ErrorTimestamp' in cleaner:
                        resource["ImageBuilderErrors"][image_builder_errors_counter]["ErrorTimestamp"] = resource[
                            "ImageBuilderErrors"][image_builder_errors_counter]["ErrorTimestamp"].isoformat()
                        image_builder_errors_counter = image_builder_errors_counter + 1

                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(
                inventory,
                file_path,
                generate_parquet_prefix(
                    str(stack()[0][3]), region, account_id, idx),
            )
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
