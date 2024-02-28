
import boto3
from utils.utils import *


def list_well_architect(file_path,session,region):
    try:
        wa_client = session.client('wellarchitected',region_name=region)
        sts = session.client('sts')
        account_id = sts.get_caller_identity()["Account"]
        inventory_instances = []
        inventory = wa_client.list_workloads()['WorkloadSummaries']
        if len(inventory) != 0:
            for i in inventory:
                wa = wa_client.get_workload(WorkloadId=i['WorkloadId'])
                arn = wa['WorkloadId']
                inventory_object = extract_common_info(arn,wa,region,account_id)
                inventory_instances.append(inventory_object)
            save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
            return inventory_instances
    except Exception as error:
        #print(f'cannot bring data of wisdom {error}')
        print(f'{error}')
   