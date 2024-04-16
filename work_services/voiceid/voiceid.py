import boto3
from utils.utils import *



def list_voiceid(file_path,session,region,time_generated,account):
    voiceid_client = session.client('voice-id',region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
    inventory_instances = []
    try:
        inventory = voiceid_client.list_domains()
        if len(inventory['DomainSummaries']) != 0:
            for i in inventory['DomainSummaries']:
               i['CreatedAt'] = i['CreatedAt'].isoformat()
               i['UpdatedAt'] = i['UpdatedAt'].isoformat()
               arn = i['Arn']
               inventory_object = extract_common_info(arn,i,region,account_id,time_generated,account_name)
               inventory_instances.append(inventory_object)
            save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
        return inventory_instances
    except Exception as e:
        print(e)