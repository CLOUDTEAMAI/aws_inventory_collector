import boto3
from utils.utils import *



def list_acm(file_path,session,region,time_generated,account):
    acm_client = session.client('acm',region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
    inventory_instances = []
    inventory = acm_client.list_certificates()
    if len(inventory['CertificateSummaryList']) != 0:
        for i in inventory['CertificateSummaryList']:
           if 'NotBefore' in i:
               i['NotBefore'] = i['NotBefore'].isoformat()
           if 'NotAfter' in i:
               i['NotAfter'] = i['NotAfter'].isoformat()
           if 'CreatedAt' in i:
               i['CreatedAt'] = i['CreatedAt'].isoformat()
           if 'IssuedAt' in i:
               i['IssuedAt'] = i['IssuedAt'].isoformat()
           if 'ImportedAt' in i:
               i['ImportedAt'] = i['ImportedAt'].isoformat()  
           if 'RevokedAt' in i:
               i['RevokedAt'] = i['RevokedAt'].isoformat()  
           arn = i['CertificateArn']
           inventory_object = extract_common_info(arn,i,region,account_id,time_generated,account_name)
           inventory_instances.append(inventory_object)
        save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    # return inventory_instances