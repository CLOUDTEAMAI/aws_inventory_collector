import boto3
from utils.utils import *

servicesNamespaces = ['ecs','elasticmapreduce','ec2','appstream','dynamodb','rds','sagemaker','custom-resource','comprehend','lambda','cassandra','kafka','elasticache','neptune']



def list_application_autoscaling(file_path,session,region,time_generated,account):
    application_autoscaling = session.client('application-autoscaling',region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
    inventory_instances = []
    for namespaces in servicesNamespaces:
        inventory = application_autoscaling.describe_scalable_targets(ServiceNamespace=namespaces)
        if len(inventory['ScalableTargets']) != 0:
            for i in inventory['ScalableTargets']:
                arn = i['ScalableTargetARN']
                inventory_object = extract_common_info(arn,i,region,account_id,time_generated,account_name)
                inventory_instances.append(inventory_object)
    if len(inventory['ScalableTargets']) != 0:
        save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    # return inventory_instances