import boto3
import pandas as pd
from datetime import datetime
from utils.utils import *

def list_elasticbeanstalk(file_path,session,region,time_generated,account):
    client = session.client('elasticbeanstalk',region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
    elasticbeanstalk_list = client.describe_applications()
    elasticbeanstalk_list_object = []
    if len(elasticbeanstalk_list['Applications']) != 0:
        for i in elasticbeanstalk_list['Applications']:

            if 'DateCreated' in i:
                i['DateCreated'] = i['DateCreated'].isoformat()
            if 'DateUpdated' in i:
                i['DateUpdated'] = i['DateUpdated'].isoformat()
            arn = i['ApplicationArn']
            object_elastic = extract_common_info(arn,i,region,account_id,time_generated,account_name)
            elasticbeanstalk_list_object.append(object_elastic)
        save_as_file_parquet(elasticbeanstalk_list_object,file_path,generate_parquet_prefix(__file__,region,account_id))
        return elasticbeanstalk_list_object
    

async def async_list_elasticbeanstalk(file_path, session, region, time_generated):
    try:
        client_list = []
        clinet_account =  session[1].client('sts')
        account_id = clinet_account.get_caller_identity()["Account"]
        async with session[0].client('ec2') as elastic:
            paginator = elastic.get_paginator('describe_applications')
            async for page in paginator.paginate():
                for i in page['Applications']:
                    if 'DateCreated' in i:
                        i['DateCreated'] = i['DateCreated'].isoformat()
                    if 'DateUpdated' in i:
                        i['DateUpdated'] = i['DateUpdated'].isoformat()

                    arn = i['ApplicationArn']
                    inventory_object = extract_common_info(arn, i, region, account_id, time_generated)
                    client_list.append(inventory_object)
    except Exception as ex:
            print(ex)
    finally:
        if len(client_list) != 0:
                save_as_file_parquet(client_list, file_path, generate_parquet_prefix(__file__, region, account_id))

