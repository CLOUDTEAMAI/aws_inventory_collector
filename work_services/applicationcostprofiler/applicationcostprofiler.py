import boto3
from utils.utils import *



def list_applicationcostprofiler(file_path,session,region,time_generated):
    cost_profiler = session.client('application-cost-profiler',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    inventory_instances = []
    inventory = cost_profiler.list_report_definitions()
    if len(inventory['reportDefinitions']) != 0:
        for i in inventory['reportDefinitions']:
           report = i['reportId']
           arn = f"arn:aws:application-cost-profiler:{region}:{account_id}:report/{report}"
           inventory_object = extract_common_info(arn,i,region,account_id,time_generated)
           inventory_instances.append(inventory_object)
        save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    # return inventory_instances


