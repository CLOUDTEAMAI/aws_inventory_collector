# import boto3
# from utils.utils import *



# client = boto3.client('snowball')

# def list_snowball(file_path):
#     sts = boto3.client('sts')
#     session = boto3.session.Session()
#     region = session.region_name
#     account_id = sts.get_caller_identity()["Account"]
#     snowball_list = client.describe_job()
#     resources = []
#     if len(snowball_list['CacheClusters']) != 0:
#         for i in snowball_list['CacheClusters']:
#             arn = i['ARN']
#             resouce_object = extract_common_info(arn,i,region,account_id)
#             resources.append(resouce_object)
#         save_as_file_parquet(resources,file_path,generate_parquet_prefix(__file__,region,account_id))
#         return resources