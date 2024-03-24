import boto3
import pandas as pd
from datetime import datetime
from utils.utils import *

def list_elasticbeanstalk(file_path,session,region):
    client = session.client('elasticbeanstalk',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    elasticbeanstalk_list = client.describe_applications()
    elasticbeanstalk_list_object = []
    if len(elasticbeanstalk_list['Applications']) != 0:
        for i in elasticbeanstalk_list['Applications']:

            if 'DateCreated' in i:
                i['DateCreated'] = i['DateCreated'].isoformat()
            if 'DateUpdated' in i:
                i['DateUpdated'] = i['DateUpdated'].isoformat()
            arn = i['ApplicationArn']
            object_elastic = extract_common_info(arn,i,region,account_id)
            elasticbeanstalk_list_object.append(object_elastic)
        save_as_file_parquet(elasticbeanstalk_list_object,file_path,generate_parquet_prefix(__file__,region,account_id))
        return elasticbeanstalk_list_object