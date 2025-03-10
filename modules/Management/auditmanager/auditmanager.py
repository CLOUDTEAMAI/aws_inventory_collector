# import boto3
# from utils.utils import *


# def list_auditmanager(file_path,session,region):
#     client = session.client('auditmanager',region_name=region, config=boto_config)
#     sts = session.client('sts')
#     account_id = sts.get_caller_identity()["Account"]
#     inventory_instances = []
#     inventory = client.list_assessments()
#     if len(inventory['assessmentMetadata']) != 0:
#         for i in inventory['assessmentMetadata']:
#            query_details = client.get_named_query(NamedQueryId=i)
#            arn = i['arn']
#            inventory_object = extract_common_info(arn,query_details,region,account_id)
#            inventory_instances.append(inventory_object)
#         save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
#     return inventory_instances
