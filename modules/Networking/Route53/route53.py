from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_route53(file_path, session, region, time_generated, account):
    """
    This Python function lists Route 53 hosted zones and saves the information to a file in Parquet
    format.

    :param file_path: The `file_path` parameter in the `list_route53` function is the file path where
    the inventory data will be saved. It is the location where the Parquet file containing the Route 53
    hosted zones information will be stored
    :param session: The `session` parameter in the `list_route53` function is typically an instance of
    `boto3.Session` class that represents your AWS credentials and configuration. It is used to create a
    client for the AWS Route 53 service in the specified region
    :param region: The `region` parameter in the `list_route53` function is used to specify the AWS
    region in which the Route 53 service is being accessed. This parameter is necessary for creating a
    client session in the specified region and for listing the hosted zones within that region. It helps
    to ensure that the
    :param time_generated: The `time_generated` parameter in the `list_route53` function is used to
    specify the timestamp or time at which the inventory data is generated or collected. This timestamp
    is important for tracking when the data was retrieved and can be useful for auditing or monitoring
    purposes. It helps in identifying the age of
    :param account: The `account` parameter in the `list_route53` function seems to be a dictionary
    containing information about an AWS account. It likely includes the keys 'account_id' and
    'account_name', which are used within the function to retrieve specific details related to the
    account for processing Route 53 hosted zones
    """
    next_token = None
    idx = 0
    client = session.client('route53', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_hosted_zones(
                Marker=next_token) if next_token else client.list_hosted_zones()
            for resource in response.get('HostedZones', []):
                arn = f"arn:aws:route53::{':'.join(resource['Id'].split('/'))}"
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path,
                                 generate_parquet_prefix(str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextMarker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def list_route53_resolver(file_path, session, region, time_generated, account):
    """
    This Python function lists Route 53 Resolver endpoints and saves the information to a file in
    Parquet format.

    :param file_path: The `file_path` parameter is the file path where the Route 53 Resolver inventory
    data will be saved. This should be a valid file path on your system where the data will be stored
    :param session: The `session` parameter in the `list_route53_resolver` function is typically an
    instance of a boto3 session that allows you to create service clients and resources for AWS
    services. It is used to create a client for the Route 53 Resolver service in the specified region
    :param region: The `region` parameter in the `list_route53_resolver` function is used to specify the
    AWS region in which the Route 53 Resolver service is being accessed. This parameter determines the
    geographical location where the Route 53 Resolver endpoints are listed and processed. It is
    important to provide the correct region where
    :param time_generated: The `time_generated` parameter in the `list_route53_resolver` function is
    used to specify the timestamp or time at which the inventory data is generated or collected. This
    parameter is important for tracking when the data was retrieved and can be useful for auditing or
    monitoring purposes. It helps in maintaining a record
    :param account: The `account` parameter in the `list_route53_resolver` function seems to be a
    dictionary containing information about an AWS account. It likely includes the keys 'account_id' and
    'account_name', which are used within the function to extract specific details for processing Route
    53 Resolver endpoints
    """
    next_token = None
    idx = 0
    client = session.client('route53resolver', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_resolver_endpoints(
                NextToken=next_token) if next_token else client.list_resolver_endpoints()
            for resource in response.get('ResolverEndpoints', []):
                arn = resource.get('Arn', '')
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path,
                                 generate_parquet_prefix(str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def list_route53_resolver_ipaddresses(file_path, session, region, time_generated, account):
    """
    This Python function lists Route 53 Resolver IP addresses for a given account and saves the
    information to a file in Parquet format.

    :param file_path: The `file_path` parameter is a string that represents the file path where the
    output data will be saved
    :param session: The `session` parameter in the `list_route53_resolver_ipaddresses` function is
    typically an AWS session object that is used to create a client for the Route 53 Resolver service in
    a specific region. This session object is usually created using the `boto3` library in Python and is
    used
    :param region: The `region` parameter in the `list_route53_resolver_ipaddresses` function is used to
    specify the AWS region in which the Route 53 Resolver service is located. This parameter is
    necessary for creating the Route53Resolver client in the specified region and for listing resolver
    endpoints and their IP addresses within that
    :param time_generated: The `time_generated` parameter in the `list_route53_resolver_ipaddresses`
    function is used to specify the timestamp or time at which the inventory data is generated. This
    parameter is important for tracking when the data was collected and can be used for various purposes
    such as auditing, monitoring, or historical analysis
    :param account: The `account` parameter in the `list_route53_resolver_ipaddresses` function seems to
    be a dictionary containing information about the account. It likely includes keys such as
    'account_id' and 'account_name'. This parameter is used to extract the account ID and account name
    to be used in the function
    """
    next_token = None
    sub_next_token = None
    idx = 0
    client = session.client('route53resolver', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_resolver_endpoints(
                NextToken=next_token) if next_token else client.list_resolver_endpoints()
            for resource in response.get('ResolverEndpoints', []):
                while True:
                    try:
                        resource_response = client.list_resolver_endpoint_ip_addresses(
                            NextToken=next_token, ResolverEndpointId=resource.get('Id')) if sub_next_token else client.list_databases(DatabaseName=resource.get('DatabaseName'))
                        for ip in resource_response.get('IpAddresses', []):
                            arn = f"{resource.get('Arn')}-{ip.get('IpId')}"
                            inventory_object = extract_common_info(
                                arn, ip, region, account_id, time_generated, account_name)
                            inventory.append(inventory_object)
                        sub_next_token = response.get('NextToken', None)
                        idx = idx + 1
                        if not sub_next_token:
                            break
                    except Exception as e:
                        print(e)
                        break
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def list_route53_profiles(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('route53profiles', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_profiles(
                NextToken=next_token) if next_token else client.list_profiles()
            for resource in response.get('ProfileSummaries', []):
                arn = resource.get('Arn')
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def list_route53_profiles_associations(file_path, session, region, time_generated, account):
    """
    This Python function lists Route 53 profile associations and saves the information to a file in
    Parquet format.

    :param file_path: The `file_path` parameter is the file path where the output data will be saved. It
    should be a string representing the location where the data will be stored, for example,
    "/path/to/output/file.parquet"
    :param session: The `session` parameter in the `list_route53_profiles_associations` function is
    typically an instance of a boto3 session that is used to create a client for the AWS service. It
    allows you to make API calls to the AWS Route 53 Profiles service in the specified region
    :param region: The `region` parameter in the `list_route53_profiles_associations` function refers to
    the AWS region where the Route 53 Profiles service is located. This parameter is used to specify the
    region when creating a client for the Route 53 Profiles service and when constructing the ARN
    (Amazon Resource Name
    :param time_generated: The `time_generated` parameter in the `list_route53_profiles_associations`
    function is used to specify the timestamp or time at which the inventory data is generated or
    collected. This parameter is important for tracking when the data was retrieved and can be useful
    for auditing or monitoring purposes. It helps in maintaining
    :param account: The `account` parameter in the `list_route53_profiles_associations` function seems
    to be a dictionary containing information about an AWS account. It likely includes keys such as
    'account_id' and 'account_name' which are used within the function to retrieve specific details
    related to the account for processing Route
    """
    next_token = None
    idx = 0
    client = session.client('route53profiles', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_profile_associations(
                NextToken=next_token) if next_token else client.list_profile_associations()
            for resource in response.get('ProfileAssociations', []):
                if 'CreationTime' in resource:
                    resource['CreationTime'] = resource['CreationTime'].isoformat(
                    )
                if 'ModificationTime' in resource:
                    if 'CreatedAt' in resource['ModificationTime']:
                        resource['ModificationTime'] = resource['LastUpdate'].isoformat(
                        )
                arn = f"arn:aws:route53profiles:{region}:{account_id}:profile-association/{resource['id']}"
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
