import boto3
from utils.utils import *



def list_autoscaling(file_path,session,region,time_generated,account):
    client = session.client('autoscaling',region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
    inventory_instances = []
    inventory = client.describe_auto_scaling_groups()
    if len(inventory['AutoScalingGroups']) != 0:
        for i in inventory['AutoScalingGroups']:
           
           if 'CreatedTime' in i:
               i['CreatedTime'] = i['CreatedTime'].isoformat()
               
           arn = i['AutoScalingGroupARN']
           inventory_object = extract_common_info(arn,i,region,account_id,time_generated,account_name)
           inventory_instances.append(inventory_object)
        save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    return inventory_instances


async def async_list_autoscaling_group(file_path, session, region, time_generated):
    try:
        client_list = []
        # client_session = session[1].client('ec2')
        clinet_account =  session[1].client('sts')
        account_id = clinet_account.get_caller_identity()["Account"]
        async with session[0].client('autoscaling') as elastic:
            paginator = await elastic.describe_auto_scaling_groups()
            for page in paginator['AutoScalingGroups']:
                if 'CreatedTime' in page:
                    page['CreatedTime'] = page['CreatedTime'].isoformat()
                arn = page['AutoScalingGroupARN']
                inventory_object = extract_common_info(arn, page, region, account_id, time_generated)
                client_list.append(inventory_object)
    except Exception as ex:
            print(ex)
    finally:
        if len(client_list) != 0:
                save_as_file_parquet(client_list, file_path, generate_parquet_prefix(__file__, region, account_id))


