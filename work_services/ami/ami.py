import boto3
from utils.utils import *

def list_ami(file_path,session,region,time_generated,account):
    try:
        client_boto     = session.client('ec2',region_name=region)
        account_id      = account['account_id']
        account_name    = str(account['account_name']).replace(" ","_")
        ami_list        = client_boto.describe_images(Owners=['self'])
        amilist         = []
        region          = region
        if len(ami_list['Images']) != 0:
            for i in ami_list['Images']:
                arn = f"arn:aws:ec2:{region}:{account_id}:image/{i['ImageId']}"
                object_client = extract_common_info(arn,i,region,account_id,time_generated,account_name)
                amilist.append(object_client)
            save_as_file_parquet(amilist,file_path,generate_parquet_prefix(__file__,region,account_id))
        # return amilist
    except Exception as ex:
        print(ex)
    