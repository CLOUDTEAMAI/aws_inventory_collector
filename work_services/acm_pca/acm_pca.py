import boto3
from utils.utils import *



def list_acm_pca(file_path,session,region,time_generated,account):
    acm_client = session.client('acm-pca',region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
    inventory_instances = []
    inventory = acm_client.list_certificate_authorities()
    if len(inventory['CertificateAuthorities']) != 0:
        for i in inventory['CertificateAuthorities']:
           arn = i['Arn']
           inventory_object = extract_common_info(arn,i,region,account_id,time_generated,account_name)
           inventory_instances.append(inventory_object)
        save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    # return inventory_instances