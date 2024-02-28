import boto3
from utils.utils import *



def list_acm_pca(file_path,session,region):
    acm_client = session.client('acm-pca',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    inventory_instances = []
    inventory = acm_client.list_certificate_authorities()
    if len(inventory['CertificateAuthorities']) != 0:
        for i in inventory['CertificateAuthorities']:
           arn = i['Arn']
           inventory_object = extract_common_info(arn,i,region,account_id)
           inventory_instances.append(inventory_object)
        save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    return inventory_instances