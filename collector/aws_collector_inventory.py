import asyncio
import threading
import aioboto3
import boto3
# from utils.utils import *
from datetime import datetime, timedelta
from cloudteam_logger import cloudteam_logger
from work_services import *
import threading
import os

lock = threading.Lock()

semaphore = asyncio.Semaphore(10)
class AWSInventoryCollector:
    def __init__(self,main_dir,logger_obj=None,time_generated=None):
        self.main_dir=main_dir
        self.logger_obj=logger_obj
        self.time_generated = time_generated or datetime.now().strftime("%Y-%m-%d %H:%M:%S")



    async def async_get_all_accounts_inventory_global(self,account_json):
        tasks = []
        for account in account_json['accounts']:
            session = await async_get_aws_session(account['account_id'])
            if session:
                task = asyncio.create_task(
                    self.__parallel_executor_inventory_global(account['account_id'])
                )
                tasks.append(task)

        await asyncio.gather(*tasks,return_exceptions=True)

    async def async_get_all_accounts_inventory(self,account_json):
        tasks = []
        for account in account_json['accounts']:
            session = await async_get_aws_session(account['account_id'])
            if session:
                regions =  regions_enabled(session[1])
                for region in regions:
                    task = asyncio.create_task(
                        self.__parallel_executor_inventory(account['account_id'], region),
                    )
                    tasks.append(task)

        await asyncio.gather(*tasks,return_exceptions=True)

    async def __parallel_executor_inventory(self,account_id, region):
        async with semaphore:
            session = await async_get_aws_session(account_id, region)

            if session:
                print(f"{account_id} {region}")
                try:
                    tasks = [

                        async_list_efs_file_systems(self.main_dir, session, region, self.time_generated),
                        async_list_autoscaling_group(self.main_dir, session, region, self.time_generated),
                        async_list_snapshot(self.main_dir, session, region, self.time_generated),
                        async_list_eip(self.main_dir, session, region, self.time_generated),
                        async_list_dynamo(self.main_dir, session, region, self.time_generated),
                        async_list_elasticbeanstalk(self.main_dir, session, region, self.time_generated),
                        async_list_eks(self.main_dir, session, region, self.time_generated),
                        async_list_ecs_clusters(self.main_dir, session, region, self.time_generated),
                        async_list_appflow(self.main_dir, session, region, self.time_generated),
                        async_list_appconfig(self.main_dir, session, region, self.time_generated),
                        async_list_apigatewayv2(self.main_dir, session, region, self.time_generated),
                        async_list_elasticahe(self.main_dir, session, region, self.time_generated),
                        async_list_ecr(self.main_dir, session, region, self.time_generated),
                        async_list_volumes(self.main_dir, session, region, self.time_generated),
                        async_list_apigateway(self.main_dir, session, region, self.time_generated),
                        async_list_ec2(self.main_dir, session, region, self.time_generated)
                    ]


                    # Call specific service functions for the given region and account
                    await asyncio.gather(*tasks,return_exceptions=True)
                except Exception as ex:
                    print(ex)
                # Add other service calls as needed, for example:
                # await list_s3(main_dir, session, region, time_generated)
                # await list_rds(main_dir, session, region, time_generated)

    async def __parallel_executor_inventory_global(self,account_id):
            async with semaphore:
                session = await async_get_aws_session(account_id)
                if session:
                    print(f"{account_id}")
                    try:
                        tasks = [
                            async_list_s3_buket(self.main_dir, session, self.time_generated)
                        ]
                        await asyncio.gather(*tasks,return_exceptions=True)
                    except Exception as ex:
                        print(ex)
    
    def get_all_accounts_inventory(self,logger_obj,main_dir: str,account_json: list,time_generated):
        try:
            max_worker = len(account_json['accounts'])
            if  max_worker > 6:
                max_worker = 5
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_worker) as executor:
                futures_services = {
                    executor.submit(
                        lambda acc=account, reg=region: self.parallel_executor_inventory(
                            logger_obj,main_dir, get_aws_session(acc['account_id'], reg), reg,time_generated
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
        



    def parallel_executor_inventory(self,logger_obj,main_dir: str,session,region: str,time_generated: datetime):
# Initialize functions clients for services you want to list resources from in parallel 
        tasks = {
             'ec2'                       : list_ec2,
             'snapshot'                  : list_ec2_snapshots,
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
              'appstream'                : list_appstream,

        }
        #'amplifyuibuilder'           : list_amplifyuibuilder,
        #'applicationcostprofiler'    : list_applicationcostprofiler,
        #'autoscaling_plans'          : 
        # 'alexaforbusiness'         : list_alexaforbusiness,



        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_task = {
                executor.submit(task,main_dir,session,region,time_generated): name for name,task in tasks.items()
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


    def parallel_executor_inventory_metrics(self,logger_obj,main_dir,session,region):
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



    def get_all_accounts_metrics(self,main_dir: str,account_json: list):
        try:
            max_worker = len(account_json['accounts'])
            if  max_worker > 6:
                max_worker = 5
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_worker) as executor:
                futures_services = {
                    executor.submit(
                        lambda acc=account: self.parallel_executor_inventory_metrics(
                            main_dir, get_aws_session(acc['account_id']),
                        ), account  # Defaulting acc and reg inside lambda
                    ): account for account in account_json['accounts'] 
                }
                for future in concurrent.futures.as_completed(futures_services):
                    try:
                        result = future.result()  # Corrected to call .result() on future
                        # Process result or print
                    except Exception as ex:
                        print(f'Error raisd in  \n {ex}')

        except Exception as ex:
            print(f'Error in end {ex}')



    def get_all_accounts_s3(self,main_dir: str,account_json: list,time_generated=None):
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(account_json['accounts'])) as executor:
            futures = []
            for account in account_json['accounts']:
                future = executor.submit(
                    lambda acc=account: metrics_utill_s3(main_dir,get_aws_session(acc['account_id']),time_generated)
                )
                futures.append(future)
            for fu in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    # print(result)
                except Exception as ex:
                    print(ex)
                    # with lock:
                        # logger_obj.error(ex)