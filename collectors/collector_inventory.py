from concurrent.futures import as_completed, ThreadPoolExecutor
from threading import Lock
from modules import *

lock = Lock()


def inventory_collector(uploads_directory, logger, accounts_json, time_generated, threads=4):
    # regional scraping per account
    try:
        get_all_accounts_regional_inventory(main_dir=uploads_directory, logger_obj=logger,
                                            account_json=accounts_json, time_generated=time_generated, threads=threads)
        print("Finished Collecting regional inventory")
    except Exception as ex:
        print(f"Failed to execute get_all_accounts_regional_inventory \n{ex}")

    # # global scraping per account
    # try:
    #     get_all_accounts_global_inventory(main_dir=uploads_directory, logger_obj=logger,
    #                                       account_json=accounts_json, time_generated=time_generated)
    #     print("Finished Collecting global inventory")
    # except Exception as ex:
    #     # logger_obj.error(str(ex))
    #     print(f"Failed to write logger global inventory {ex}")


def get_all_accounts_regional_inventory(logger_obj, main_dir: str, account_json: list, time_generated, threads=4):
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
                ): account for account in account_json['accounts'] for region in regions_enabled(get_aws_session(account['account_id'], role_name=account['account_role']))
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
    # Initialize functions clients for services you want to list resources from in parallel
    functions_map = {
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
        'sagemaker': list_sagemaker,
        'ssm': list_ssm,
        'transferfamily': list_transferfamily,
        'capacityreservations': list_capacityreservations,
        'ec2_reservations': list_ec2_reservations,
        'rds_reservations': list_rds_reservations,
        'opensearch_reservations': list_opensearch_reservations,
        'elasticsearch_reservations': list_elasticsearch_reservations,
        'memorydb_reservations': list_memorydb_reservations,
        'elasticcache_reservations': list_elasticcache_reservations,
        'redshift_reservations': list_redshift_reservations,
        'opensearch': list_opensearch_domains,
        'elasticsearch': list_elasticsearch_domains,
        'memorydb': list_memorydb,
        'ads_agents': list_ads_agents,
        # 'internetgateway': list_internetgateway,
        'eip': list_eip,
        'natgateway': list_natgateway,
        'redshift': list_redshift,
        'redshift_serverless': list_redshift_serverless,
        'emr': list_emr,
        'emr_instance_group': list_emr_instance_group,
        'emr_containers': list_emr_containers,
        'emr_containers_jobs': list_emr_containers_jobs,
        'emr_serverless_jobs': list_emr_serverless_jobs,
        'kinesis': list_kinesis,
        'rekognition': list_rekognition,
        'elastic_cache': list_cache,
        'elb': list_elb,
        'elbv2': list_elbv2,
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
        'neptune': list_neptune,
        'neptune_instances': list_neptune_instances,
        'waf': list_waf,
        'appconfig': list_appconfig,
        'apigatewayv2': list_apigatewayv2,
        'acm': list_acm,
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
        # 'routetable': list_routetable,
        'wisdom': list_wisdom,
        'voiceid': list_voiceid,
        'appstream': list_appstream,
        'transitgateway': list_transitgateway,
        'transitgatewayattachments': list_transitgateway_attachments,
        'vpcendpoints': list_vpc_endpoint,
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
