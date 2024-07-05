from concurrent.futures import as_completed, ThreadPoolExecutor
from threading import Lock
from .metrics import *

lock = Lock()


def metrics_collector(uploads_directory, logger, accounts_json, time_generated, metrics, threads=4):
    # regional scraping per account
    try:
        get_all_accounts_metrics(main_dir=uploads_directory, logger_obj=logger,
                                 account_json=accounts_json, time_generated=time_generated, metrics=metrics, threads=threads)
        print("Finished Collecting regional inventory")
    except Exception as ex:
        print(f"Failed to execute get_all_accounts_regional_inventory \n{ex}")


def parallel_executor_inventory_metrics(logger_obj, main_dir, session, region, account, time_generated, metrics):
    tasks = {}
    global_tasks = {}
    functionsz_map = {
        'workspaces_metrics': workspaces_metrics,
        'ec2_instances_metrics': ec2_instances_metrics,
        'ec2_instances_cwagent_metrics': ec2_instances_cwagent_metrics,
        'ebs_volumes_metrics': ebs_volumes_metrics,
        'functions_metrics': functions_metrics,
        'efs_filesystem_metrics': efs_filesystem_metrics,
        'amplify_metrics': amplify_metrics,
        'datasync_agents_metrics': datasync_agents_metrics,
        'ecr_metrics': ecr_metrics,
        'cloudhsmv2_metrics': cloudhsmv2_metrics,
        'fsx_filesystem_metrics': fsx_filesystem_metrics,
        'rds_instances_metrics': rds_instances_metrics,
        'rds_proxies_metrics': rds_proxies_metrics,
        'elasticache_metrics': elasticache_metrics,
        'opensearch_metrics': opensearch_metrics,
        'dynamodb_tables_metrics': dynamodb_tables_metrics,
        'sqs_metrics': sqs_metrics,
        'sns_metrics': sns_metrics,
        'mq_metrics': mq_metrics,
        'route53_resolver_metrics': route53_resolver_metrics,
        'msk_nodes_metrics': msk_nodes_metrics,
        'vpcendpoint_metrics': vpcendpoint_metrics,
        'clientvpn_metrics': clientvpn_metrics,
        'vpn_metrics': vpn_metrics,
        'networkfirewall_metrics': networkfirewall_metrics,
        'natgateway_metrics': natgateway_metrics,
        'elb_metrics': elb_metrics,
        'elbv2_network_metrics': elbv2_network_metrics,
        'elbv2_application_metrics': elbv2_application_metrics,
        'transitgateway_metrics': transitgateway_metrics,
        'transitgateway_attachments_metrics': transitgateway_attachments_metrics,
        'transferfamily_metrics': transferfamily_metrics
    }
    if region == 'us-east-1':
        global_tasks = {
            'route53_metrics': route53_metrics,
            'cloudfront_metrics': cloudfront_metrics,
            's3_metrics': s3_metrics
        }
    functions_map = {**functionsz_map, **global_tasks}
    # elif region == 'us-west-2':
    #     global_tasks = {'globalaccelerator': list_globalaccelerator}

    # for namespace in metrics:
    #     for metric in metrics[namespace]:
    #         if metric['list_function'] in functions_map.keys():
    #             tasks[metric['list_function']] = []

    for namespace in metrics:
        for metric in metrics[namespace]:
            if metric['list_function'] in functions_map.keys():
                if not tasks.get(metric['list_function']):
                    tasks[metric['list_function']] = []
                tasks[metric['list_function']].append(
                    {
                        "days_ago": metric['days_ago'],
                        "granularity_seconds": metric['granularity_seconds'],
                        "aws_namespace": namespace,
                        "aws_dimensions": metric.get('aws_dimensions', []),
                        "aws_dimensions_name": metric['aws_dimensions_name'],
                        "aws_metric_name": metric['aws_metric_name'],
                        "aws_unit": metric['aws_unit'],
                        "aws_statistics": metric['aws_statistics']
                    }
                )

    with ThreadPoolExecutor() as executor:
        future_to_task = {
            executor.submit(functions_map[name], main_dir, session, region, account, task, time_generated): name for name, task in tasks.items()
        }
        for future in as_completed(future_to_task):
            task_name = future_to_task[future]
            try:
                data = future.result()
                print(f"{task_name} completed, {data}")
                del data
            except Exception as exc:
                print(f"{task_name} generated an exception: {exc}")
                sts = session.client('sts')
                account_id = sts.get_caller_identity()['Account']
                with lock:
                    logger_obj.error(
                        f'{account_id} {region} {task_name} {str(exc)}')


def get_all_accounts_metrics(logger_obj, main_dir: str, account_json: list, time_generated, metrics, threads=4):
    try:
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures_services = {}
            for account in account_json['accounts']:
                session = get_aws_session(
                    account['account_id'], role_name=account['account_role'])
                if session is not None:
                    regions = regions_enabled(session)
                    for region in regions:
                        future = executor.submit(
                            lambda session=get_aws_session(
                                account['account_id'], role_name=account['account_role'], region=region), acc=account, reg=region: parallel_executor_inventory_metrics(
                                logger_obj,
                                main_dir,
                                session,
                                reg,
                                complete_aws_account(acc),
                                time_generated,
                                metrics
                            )
                        )
                        futures_services[future] = account

            for future in as_completed(futures_services):
                try:
                    result = future.result()  # Retrieve the result
                    del result
                    # Process result or print
                except Exception as ex:
                    print(f'Error raised in \n {ex}')
                    with lock:
                        logger_obj.error(ex)

    except Exception as ex:
        print(f'Error in end {ex}')
