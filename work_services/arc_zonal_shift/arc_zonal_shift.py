import boto3
from utils.utils import *



def list_arc_zonal_shift(file_path,session,region):
    client = session.client('arc-zonal-shift',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    inventory_instances = []
    inventory = client.list_managed_resources()
    if len(inventory['items']) != 0:
        for i in inventory['items']:
           
           
           for arc in i.get('autoshifts',[]):
               arc['startTime'] = arc['startTime'].isformat()
            
           for zonal in i.get('zonalShifts',[]):
                zonal['expiryTime'] = zonal['expiryTime'].isformat()
                zonal['startTime'] = zonal['startTime'].isformat()

           query_details = client.get_named_query(NamedQueryId=i)
           arn = i['arn']
           inventory_object = extract_common_info(arn,query_details,region,account_id)
           inventory_instances.append(inventory_object)
        save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    return inventory_instances


