from concurrent.futures import as_completed, ThreadPoolExecutor
from threading import Lock
from .inventory import *

lock = Lock()


def inventory_collector(uploads_directory, logger, accounts_json, time_generated, threads=4):
    """
    The function `inventory_collector` collects regional inventory data for multiple accounts using
    multithreading and logs any errors encountered.

    :param uploads_directory: The `uploads_directory` parameter is a string that represents the main
    directory where the inventory uploads are stored or where the inventory collection process will save
    its output files
    :param logger: The `logger` parameter in the `inventory_collector` function is typically an object
    that is used for logging information, warnings, errors, and other messages during the execution of
    the function. It helps in tracking the progress of the function, identifying issues, and debugging
    problems
    :param accounts_json: The `accounts_json` parameter in the `inventory_collector` function is likely
    a JSON file containing account information or configurations needed for the inventory collection
    process. This JSON file could include details such as account credentials, API keys, or other
    settings required to access and retrieve inventory data from different accounts or regions
    :param time_generated: The `time_generated` parameter in the `inventory_collector` function is used
    to specify the time at which the inventory collection process is initiated or generated. This
    parameter likely represents a timestamp or datetime object indicating when the inventory data is
    being collected. It helps in tracking and organizing the inventory data based on
    :param threads: The `threads` parameter in the `inventory_collector` function specifies the number
    of threads to use for concurrent processing while collecting regional inventory data for accounts.
    By default, it is set to 4, meaning that the function will use 4 threads for this task unless a
    different value is explicitly provided, defaults to 4 (optional)
    """
    # regional scraping per account
    try:
        get_all_accounts_regional_inventory(main_dir=uploads_directory, logger_obj=logger,
                                            account_json=accounts_json, time_generated=time_generated, threads=threads)
        print("Finished Collecting regional inventory")
    except Exception as ex:
        print(f"Failed to execute get_all_accounts_regional_inventory \n{ex}")


def get_all_accounts_regional_inventory(logger_obj, main_dir: str, account_json: list, time_generated, threads=4):
    """
    The function `get_all_accounts_regional_inventory` uses ThreadPoolExecutor to concurrently retrieve
    regional inventory data for multiple AWS accounts and regions.

    :param logger_obj: The `logger_obj` parameter is an object that is used for logging messages and
    errors in the code. It is typically an instance of a logging class that allows you to record events
    that occur during the program's execution. This object helps in tracking and debugging the code by
    providing a way to store and
    :param main_dir: The `main_dir` parameter in the `get_all_accounts_regional_inventory` function is a
    string that represents the main directory where the inventory data will be stored or retrieved from.
    It is used as a path or directory location within the file system where the function will read or
    write data related to the
    :type main_dir: str
    :param account_json: The `account_json` parameter in the `get_all_accounts_regional_inventory`
    function seems to be a list of dictionaries. Each dictionary in the list likely represents an
    account with keys like 'account_id' and 'account_role'. This list is used to iterate over accounts
    and their regions to perform some
    :type account_json: list
    :param time_generated: The `time_generated` parameter in the `get_all_accounts_regional_inventory`
    function seems to be used to specify a timestamp or time-related information. This parameter likely
    indicates the time at which the inventory data is being generated or collected for the accounts and
    regions
    :param threads: The `threads` parameter in the `get_all_accounts_regional_inventory` function
    specifies the maximum number of threads that can be used by the ThreadPoolExecutor for executing the
    tasks concurrently. In this case, the default value for `threads` is set to 4, meaning that at most
    4 tasks, defaults to 4 (optional)
    """
    try:
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures_services = {
                executor.submit(
                    lambda acc=account, reg=region: parallel_executor_regional_inventory(
                        logger_obj, main_dir,
                        get_aws_session(acc['account_id'],
                                        reg, role_name=acc['account_role']),
                        reg,
                        time_generated,
                        complete_aws_account(acc),
                        threads
                    ), account, region  # Defaulting acc and reg inside lambda
                ): account for account in account_json['accounts'] for region in regions_enabled(get_aws_session(account['account_id'], role_name=account['account_role'])) if get_aws_session(account['account_id'], role_name=account['account_role']) is not None
            }
            for future in as_completed(futures_services):
                try:
                    result = future.result()  # Corrected to call .result() on future
                    # Process result or print
                    del result
                except Exception as ex:
                    print(f'Error raised in \n {ex}')
                    with lock:
                        logger_obj.error(str(ex))
    except Exception as ex:
        print(f'Error in end {ex}')


def parallel_executor_regional_inventory(logger_obj, main_dir: str, session, region: str, time_generated: datetime, account, threads=4):
    """
    The function `parallel_executor_regional_inventory` executes multiple resource listing functions in
    parallel for a specified region and account.

    :param logger_obj: The `logger_obj` parameter in the `parallel_executor_regional_inventory` function
    is an object that handles logging messages and errors during the execution of the function. It is
    used to log information about the tasks being executed, any exceptions that occur, and other
    relevant details for monitoring and debugging purposes
    :param main_dir: The `main_dir` parameter in the `parallel_executor_regional_inventory` function is
    a string that represents the main directory path where the function will list resources from various
    services in parallel. This directory path is used as a reference point for accessing and organizing
    the resources being listed by the function
    :type main_dir: str
    :param session: The `session` parameter in the `parallel_executor_regional_inventory` function is
    typically an object representing a session with the AWS services. It is commonly used to make API
    calls to AWS services within the specified region. This session object usually contains the
    necessary credentials and configuration settings required to authenticate and interact with
    :param region: The `region` parameter in the `parallel_executor_regional_inventory` function is used
    to specify the AWS region for which you want to list resources. This function is designed to list
    resources from various AWS services in parallel for the specified region. The function contains a
    mapping of service names to corresponding list functions
    :type region: str
    :param time_generated: The `time_generated` parameter in the `parallel_executor_regional_inventory`
    function is expected to be of type `datetime`. This parameter represents the timestamp or datetime
    at which the inventory is being generated or processed. It is used within the function to provide
    context or time reference for the inventory operations being performed
    :type time_generated: datetime
    :param account: The `account` parameter in the `parallel_executor_regional_inventory` function seems
    to represent an account object or dictionary containing information about an AWS account. It likely
    includes details such as the account ID, account name, and possibly other account-specific
    information needed for listing AWS resources in parallel for that account within
    :param threads: The `threads` parameter in the `parallel_executor_regional_inventory` function
    specifies the number of threads to use for parallel execution of tasks. Increasing the number of
    threads can potentially improve performance by allowing multiple tasks to run concurrently. However,
    it's important to consider the resources available and the nature of the, defaults to 4 (optional)
    """
    # Initialize functions clients for services you want to list resources from in parallel
    functions_map = {
        'dlm': list_dlm,
        'logs': list_logs_groups,
        'ec2': list_ec2,
        'ami': list_ami,
        'snapshot': list_ec2_snapshots,
        'snapshots_fsr': list_ec2_snapshots_fsr,
        'sqs': list_sqs,
        'sns': list_sns,
        'sns_subscriptions': list_sns_subscriptions,
        'vpc': list_vpc,
        'eni': list_eni,
        'vpn_gateway': list_vpn_gateway,
        'vpn_connections': list_vpn_connections,
        'clientvpn': list_clientvpn,
        'clientvpn_connections': list_clientvpn_connections,
        'sagemaker_cluster': list_sagemaker_cluster,
        'sagemaker_domain': list_sagemaker_domain,
        'sagemaker_notebook_instance': list_sagemaker_notebook_instance,
        'sagemaker_endpoint': list_sagemaker_endpoint,
        'sagemaker_model': list_sagemaker_models,
        'sagemaker_training_job': list_sagemaker_training_jobs,
        'sagemaker_hpt_job': list_sagemaker_hpt_jobs,
        'ssm': list_ssm,
        'transferfamily': list_transferfamily,
        'capacityreservations': list_capacityreservations,
        'ec2_reservations': list_ec2_reservations,
        'rds_reservations': list_rds_reservations,
        'opensearch_reservations': list_opensearch_reservations,
        'elasticsearch_reservations': list_elasticsearch_reservations,
        'memorydb_reservations': list_memorydb_reservations,
        'elasticcache_reservations': list_elasticcache_reservations,
        'elasticache_sizing': list_cache_sizing,
        'redshift_reservations': list_redshift_reservations,
        'opensearch': list_opensearch_domains,
        'elasticsearch': list_elasticsearch_domains,
        'memorydb': list_memorydb,
        'memorydb_snapshots': list_memorydb_snapshots,
        'ads_agents': list_ads_agents,
        'timestream_influxdb_instances': list_timestream_influxdb_instances,
        'timestream_write_databases': list_timestream_write_databases,
        'timestream_write_tables': list_timestream_write_tables,
        'directconnect_connections': list_directconnect_connections,
        'directconnect_interfaces': list_directconnect_interfaces,
        # 'internetgateway': list_internetgateway,
        'eip': list_eip,
        'natgateway': list_natgateway,
        'redshift': list_redshift,
        'redshift_cluster_snapshots': list_redshift_cluster_snapshots,
        'redshift_serverless': list_redshift_serverless,
        'redshift_serverless_snapshots': list_redshift_serverless_snapshots,
        'emr': list_emr,
        'emr_instance_group': list_emr_instance_group,
        'emr_containers': list_emr_containers,
        'emr_containers_jobs': list_emr_containers_jobs,
        'emr_serverless_jobs': list_emr_serverless_jobs,
        'kinesis': list_kinesis,
        'rekognition': list_rekognition,
        'elastic_cache': list_cache,
        'elastic_cache_serverless': list_cache_serverless,
        'elasticache_serverless_snapshots': list_cache_serverless_snapshots,
        'elasticache_snapshots': list_cache_snapshots,
        'docdb_global': list_docdb_global,
        'docdb': list_docdb,
        'docdb_instances': list_docdb_instances,
        'docdb_cluster_snapshots': list_docdb_cluster_snapshots,
        'docdb_elastic_cluster': list_docdb_elastic_cluster,
        'docdb_elastic_cluster_snapshots': list_docdb_elastic_cluster_snapshots,
        'elb': list_elb,
        'elbv2': list_elbv2,
        'elbv2_target_group': list_elbv2_target_group,
        'elasticbeanstalk': list_elasticbeanstalk,
        'amplify': list_amplify,
        'lambdafunctions': list_lambda,
        'ebs': list_volumes,
        'networkfirewall':  list_networkfirewall,
        'batch_compute': list_batch_compute,
        'batch_jobs': list_batch_jobs,
        'firehose': list_firehose,
        'ecr': list_ecr_repositories,
        'ecr_images': list_ecr_repositories_images,
        'ecs': list_ecs_clusters,
        'efs': list_efs_file_systems,
        'datasync_agents': list_datasync_agents,
        'rds': list_rds,
        'rds_global': list_rds_global,
        'rds_instances': list_rds_instances,
        'rds_snapshots': list_rds_snapshots,
        'rds_proxies': list_rds_proxies,
        'rds_proxy_endpoints': list_rds_proxy_endpoints,
        'msk': list_msk,
        'msk_nodes': list_msk_nodes,
        'appintegrations': list_appintegrations,
        'application_insights': list_application_insights,
        'amp': list_amp,
        'dms_tasks': list_dms_tasks,
        'dms_instances': list_dms_instances,
        'athena': list_athena,
        'apprunner': list_apprunner,
        'apigateway': list_apigateway,
        'acm_pca': list_acm_pca,
        'xray': list_xray,
        'workspace': list_workspaces,
        'workspacesthinclient': list_workspaces_thin_client,
        'eks': list_eks,
        'dynamodb': list_dynamodb,
        'dynamodb_streams': list_dynamodb_streams,
        'dax': list_dax,
        'secrets': list_secrets,
        # 'neptune': list_neptune,
        # 'neptune_instances': list_neptune_instances,
        'neptune_snapshots': list_neptune_snapshots,
        'waf': list_waf,
        'appconfig': list_appconfig,
        'apigatewayv2': list_apigatewayv2,
        'acm': list_acm,
        'mwaa': list_mwaa,
        'keyspace': list_keyspaces,
        'keyspaces_tables': list_keyspaces_tables,
        'keyspaces_tables_autoscaling': list_keyspaces_tables_autoscaling,
        # 'vpclattice': list_vpclattice,
        'timestreamwrite': list_timestreamwrite,
        'accessanalyzer': list_accessanalyzer,
        'appmesh': list_appmesh,
        'applicationautoscaling': list_application_autoscaling,
        'appflow': list_appflow,
        'appsync': list_appsync,
        'arc_zonal_shift': list_arc_zonal_shift,
        'autoscaling': list_autoscaling,
        'autoscaling_plans': list_autoscaling_plans,
        'route53_resolver': list_route53_resolver,
        'route53_profiles': list_route53_profiles,
        'route53_profiles_associations': list_route53_profiles_associations,
        # 'routetable': list_routetable,
        'wisdom': list_wisdom,
        'voiceid': list_voiceid,
        'appstream': list_appstream,
        'transitgateway': list_transitgateway,
        'transitgatewayattachments': list_transitgateway_attachments,
        'vpcendpoints': list_vpc_endpoint,
        'vpcendpointconnections': list_vpc_endpoint_services,
        'mq': list_mq,
        'fsx_fs': list_fsx_filesystems,
        'fsx_volumes': list_fsx_volumes,
        'fsx_filecache': list_fsx_filecache,
        'cloudhsmv2': list_cloudhsmv2,
        'translate': list_translate,
        #  'acl'                       : list_acl,
        # 'securitygroup': list_securitygroup,
        #   'cloudformation'           : list_cloudformation,
        #   'wellarchitected'          : list_well_architect,
    }
    if region == 'us-east-1':
        global_tasks = {
            'account_name': list_account_name,
            'accounts': list_accounts,
            's3': list_s3_buckets,
            'wafv2': list_wafv2,
            'route53': list_route53,
            'shield': list_shield,
            'shieldprotections': list_shield_protections,
            'cloudfront': list_cloudfront,
            'savingsplans': list_savingsplans
        }
    elif region == 'us-west-2':
        global_tasks = {'globalaccelerator': list_globalaccelerator}
    else:
        global_tasks = {}
    tasks = {**functions_map, **global_tasks}
    # 'amplifyuibuilder'           : list_amplifyuibuilder,
    # 'applicationcostprofiler'    : list_applicationcostprofiler,
    # 'alexaforbusiness'         : list_alexaforbusiness,

    with ThreadPoolExecutor(threads) as executor:
        future_to_task = {
            executor.submit(task, main_dir, session, region, time_generated, account): name for name, task in tasks.items()
        }
        for future in as_completed(future_to_task):
            task_name = future_to_task[future]
            try:
                data = future.result()
                print(f"{task_name} completed {region}, {data}")
                del data
            except Exception as exc:
                print(f"{task_name} generated an exception: {exc}")
                account_id = account['account_id']
                with lock:
                    logger_obj.error(
                        f'{account_id} {region} {task_name} {str(exc)}')
                del exc
        del future_to_task
