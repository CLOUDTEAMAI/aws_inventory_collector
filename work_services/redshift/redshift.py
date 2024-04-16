import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from cloudwatch_logic import *
import os 
from utils.utils import *

   

def list_redshift(file_path,session,region,time_generated,account):
    client = session.client('redshift',region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
    client_list_redshift = client.describe_clusters()
    
    client_list = []
    if len(client_list_redshift['Clusters']) != 0:
        for i in client_list_redshift['Clusters']:
            if 'ClusterCreateTime' in i:
                i['ClusterCreateTime'] = i['ClusterCreateTime'].isoformat()
            if 'ExpectedNextSnapshotScheduleTime' in i:
                i['ExpectedNextSnapshotScheduleTime'] = i['ExpectedNextSnapshotScheduleTime'].isoformat()
            if 'NextMaintenanceWindowStartTime' in i:
                i['NextMaintenanceWindowStartTime'] = i['NextMaintenanceWindowStartTime'].isoformat()
            for time in i.get('DeferredMaintenanceWindows',[]):
                if 'DeferMaintenanceStartTime' in time:
                    time['DeferMaintenanceStartTime'] = time['DeferMaintenanceStartTime'].isoformat()
                if 'DeferMaintenanceEndTime' in time:
                    time['DeferMaintenanceEndTime'] = time['DeferMaintenanceEndTime'].isoformat()
            if 'ReservedNodeExchangeStatus' in i and 'RequestTime' in i['ReservedNodeExchangeStatus']:
                i['ReservedNodeExchangeStatus']['RequestTime'] = i['ReservedNodeExchangeStatus']['RequestTime'].isoformat()
            if 'CustomDomainCertificateExpiryDate' in i:
                i['CustomDomainCertificateExpiryDate'] = i['CustomDomainCertificateExpiryDate'].isoformat()

            arn = i['ClusterNamespaceArn']
            client_object = extract_common_info(arn,i,region,account_id,time_generated,account_name)
            client_list.append(client_object)
        save_as_file_parquet(client_list,file_path,generate_parquet_prefix(__file__,region,account_id))
    return client_list


