import boto3
import json
import concurrent.futures
from work_services import *
from cloudteam_logger import cloudteam_logger
import threading

lock = threading.Lock()





def get_all_accounts_inventory(logger_obj,main_dir: str,account_json: list):
    try:
        max_worker = len(account_json['accounts'])
        if  max_worker > 6:
            max_worker = 5
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_worker) as executor:
            futures_services = {
                executor.submit(
                    lambda acc=account, reg=region: parallel_executor_inventory(
                        logger_obj,main_dir, get_aws_session(acc['account_id'], reg), reg
                    ), account, region  # Defaulting acc and reg inside lambda
                ): account for account in account_json['accounts'] for region in regions_enabled(get_aws_session(account['account_id']))
            }
            for future in concurrent.futures.as_completed(futures_services):
                try:
                    result = future.result()  # Corrected to call .result() on future
                    # Process result or print
                except Exception as ex:
                    print(f'Error raisd in  \n {ex}')
                    with lock:
                        logger_obj.error(ex)
                    
    except Exception as ex:
        print(f'Error in end {ex}')

def parallel_executor_inventory(logger_obj,main_dir,session,region):
# Initialize functions clients for services you want to list resources from in parallel 
    tasks = {
         'snapshot'                  : list_ec2_snapshots,
         'ec2'                       : list_ec2,
         'sqs'                       : list_sqs,
         'sns'                       : list_sns,
         'vpc'                       : list_vpc,
         'eni'                       : list_eni,
         'sagemaker'                 : list_sagemaker,
         'vpc_peering'               : list_vpc_peering,
         'vpc_endpoint'              : list_vpc_endpoint,
         'ssm'                       : list_ssm,
         'shield'                    : list_shield,
         'acl'                       : list_acl,
         'internetgateway'           : list_internetgateway,
         'eip'                       : list_eip,
         'natgateway'               : list_natgateway,
         'securitygroup'             : list_securitygroup,
          'cloudformation'           : list_cloudformation,
          'redshift'                 : list_redshift,
          'emr'                      : list_emr,
          'kinesis'                  : list_kinesis,
          'rekognition'              : list_rekognition,
          'elastic_cache'            : list_cache,
          'elb'                      : list_elb,
          'elbv2'                    : list_elbv2,
          'elasticbeanstalk'         : list_elasticbeanstalk,
          'amplify'                  : list_amplify,  
          'lambdafunction'           : list_lambda,
          'ebslist'                  : list_volumes,
          'ecr'                      : list_ecr_repositories,
          'ecs'                      : list_ecs_clusters,
          'efs'                      : list_efs_file_systems,
          'rds'                      : list_rds,
          'appintegrations'          : list_appintegrations,
          'application_insights'     : list_application_insights,
          'amp'                      : list_amp,
          'athena'                   : list_athena,
          'apprunner'                : list_apprunner,
          'apigateway'               : list_apigateway,
          'acm_pca'                  : list_acm_pca,
          'xray'                     : list_xray,
          'workspace'                : list_workspaces,
          'workspacesthinclient'     : list_workspaces_thin_client,
          'eks'                      : list_eks,
          'dynamo_db'                : list_dynamo,
          'wellarchitected'          : list_well_architect,
          'wafv2'                    : list_wafv2,
          'waf'                      : list_waf,
          'appconfig'                : list_appconfig,
          'apigatewayv2'             : list_apigatewayv2,
          'acm'                      : list_acm,
          'vpclattice'               : list_vpclattice,
          'timestreamwrite'          : list_timestreamwrite,
          'route53'                  : list_route53,
          'accessanalyzer'           : list_accessanalyzer,
          'appmesh'                  : list_appmesh,
          'applicationautoscaling'   : list_application_autoscaling,
          'appflow'                  : list_appflow,
          'appsync'                  : list_appsync,
          'arc_zonal_shift'          : list_arc_zonal_shift,
          'autoscaling'              : list_autoscaling,
          'routetable'               : list_routetable,
          'wisdom'                   : list_wisdom,
          'voiceid'                  : list_voiceid,
        #   'appstream'                : list_appstream,

    }
        #'amplifyuibuilder'           : list_amplifyuibuilder,
        #'applicationcostprofiler'    : list_applicationcostprofiler,
        #'autoscaling_plans'          : 
        # 'alexaforbusiness'         : list_alexaforbusiness,



    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_task = {
            executor.submit(task,main_dir,session,region): name for name,task in tasks.items()
            }
        for future in concurrent.futures.as_completed(future_to_task):
            task_name = future_to_task[future]
            try:
                data = future.result()
                print(f"{task_name} completed {region}, {data}")
            except Exception as exc:
                print(f"{task_name} generated an exception: {exc}")
                sts = session.client('sts') 
                account_id = sts.get_caller_identity()['Account']
                with lock:
                    logger_obj.error(f'{account_id} {region} {task_name} {str(exc)}')


def get_all_accounts_s3(main_dir: str,account_json: list,logger_obj):
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(account_json['accounts'])) as executor:
        futures = []
        for account in account_json['accounts']:
            future = executor.submit(
                lambda acc=account: list_s3_buckets(main_dir,get_aws_session(acc['account_id']))
            )
            futures.append(future)
        for fu in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                # print(result)
            except Exception as ex:
                print(ex)
                with lock:
                    logger_obj.error(ex)


def parallel_executor_inventory_metrics(logger_obj,main_dir,session,region):
    tasks = {
        'ec2_metrics'                 : metrics_utill_ec2
    }
   
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_task = {
            executor.submit(task,main_dir,session,region): name for name,task in tasks.items()
            }
        for future in concurrent.futures.as_completed(future_to_task):
            task_name = future_to_task[future]
            try:
                data = future.result()
                print(f"{task_name} completed, {data}")
            except Exception as exc:
                print(f"{task_name} generated an exception: {exc}")
                sts = session.client('sts') 
                account_id = sts.get_caller_identity()['Account']
                with lock:
                    logger_obj.error(f'{account_id} {region} {task_name} {str(exc)}')





def get_all_accounts_metrics(logger_obj,main_dir: str,account_json: list):
    try:
        max_worker = len(account_json['accounts'])
        if  max_worker > 6:
            max_worker = 5
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_worker) as executor:
            futures_services = {
                executor.submit(
                    lambda acc=account, reg=region: parallel_executor_inventory_metrics(
                        logger_obj,main_dir, get_aws_session(acc['account_id'], reg), reg
                    ), account, region  # Defaulting acc and reg inside lambda
                ): account for account in account_json['accounts'] for region in regions_enabled(get_aws_session(account['account_id']))
            }
            for future in concurrent.futures.as_completed(futures_services):
                try:
                    result = future.result()  # Corrected to call .result() on future
                    # Process result or print
                except Exception as ex:
                    print(f'Error raisd in  \n {ex}')
                    with lock:
                        logger_obj.error(ex)
                    
    except Exception as ex:
        print(f'Error in end {ex}')

# Generic collector function
def collect_all_resources(services_clients, services_listing_functions):
    all_resources = {}
    for service, client in services_clients.items():
        if service in services_listing_functions:
            try:
                listing_function = services_listing_functions[service]
                resources = listing_function(client)
                all_resources[service] = resources
            except Exception as e:
                print(f"Error collecting resources from {service}: {e}")
    return all_resources










# def parallel_executor_inventory_s3(main_dir,session):
# # Initialize Boto3 clients for services you want to list resources from
#     tasks = {
#         's3'                    : list_s3_buckets,
#     }

#     with concurrent.futures.ThreadPoolExecutor() as executor:
#         future_to_task = {
#             executor.submit(task,main_dir,session): name for name,task in tasks.items()
#             }
#         for future in concurrent.futures.as_completed(future_to_task):
#             task_name = future_to_task[future]
#             try:
#                 data = future.result()
#                 print(f"{task_name} completed")
#             except Exception as exc:
#                 print(f"{task_name} generated an exception: {exc}")
#                 # sts = session.client('sts') 
#                 # account_id = sts.get_caller_identity()['Account']
#                 # with lock:
#                 #     logger_obj.error(f'{account_id} {task_name} {str(exc)}')









