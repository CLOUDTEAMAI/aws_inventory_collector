from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_sagemaker_cluster(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function lists SageMaker clusters, extracts common information, and saves the data as
    Parquet files.

    :param file_path: The `file_path` parameter in the `list_sagemaker` function is the path where the
    inventory data will be saved as a file. It is the location where the Parquet file containing the
    extracted information about SageMaker clusters will be stored
    :param session: The `session` parameter in the `list_sagemaker` function is an object representing
    the current session. It is typically created using the `boto3.Session` class and is used to create
    clients and resources for AWS services
    :param region: The `region` parameter in the `list_sagemaker` function refers to the AWS region
    where the Amazon SageMaker resources are located. This parameter is used to specify the region when
    creating a client for the SageMaker service and when extracting information about SageMaker clusters
    in that region. It is important
    :param time_generated: The `time_generated` parameter in the `list_sagemaker` function is used to
    specify the timestamp or time at which the inventory data is being generated. This timestamp is
    typically used for tracking and auditing purposes to know when the inventory information was
    collected or updated. It is important to provide an accurate
    :param account: The `account` parameter in the `list_sagemaker` function seems to be a dictionary
    containing information about the account. It likely has the following structure:
    """
    next_token = None
    idx = 0
    client = session.client(
        'sagemaker', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_clusters(
                NextToken=next_token) if next_token else client.list_clusters()
            for resource in response.get('ClusterSummaries', []):
                arn = resource['ClusterArn']
                cluster = client.describe_cluster(
                    ClusterName=resource['ClusterName'])
                cluster['CreationTime'] = cluster['CreationTime'].isoformat()
                inventory_object = extract_common_info(
                    arn, cluster, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def list_sagemaker_domain(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function lists SageMaker domains, extracts common information, and saves the data as
    Parquet files.

    :param file_path: The `file_path` parameter in the `list_sagemaker_domain` function is the path
    where the output file will be saved. It should be a string representing the file path where the
    inventory data will be stored
    :param session: The `session` parameter in the `list_sagemaker_domain` function is typically an
    instance of a boto3 session that is used to create a client for interacting with AWS services. It
    allows you to configure credentials, region, and other settings for making API calls to AWS services
    like SageMaker
    :param region: The `region` parameter in the `list_sagemaker_domain` function is used to specify the
    AWS region where the Amazon SageMaker resources are located. SageMaker resources such as domains are
    region-specific, so you need to provide the region name where you want to list the domains. For
    example,
    :param time_generated: The `time_generated` parameter in the `list_sagemaker_domain` function is
    used to specify the timestamp or time at which the inventory data is generated or collected. This
    timestamp is typically used for tracking when the inventory information was retrieved or processed.
    It helps in maintaining a record of when the data
    :param account: The `account` parameter in the `list_sagemaker_domain` function seems to be a
    dictionary containing information about the AWS account. It likely has the following structure:
    """
    next_token = None
    idx = 0
    client = session.client(
        'sagemaker', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        inventory = []
        response = client.list_domains(
            NextToken=next_token) if next_token else client.list_domains()
        for resource in response.get('Domains', []):
            arn = resource['ClusterArn']
            cluster = client.describe_domain(
                DomainId=resource['DomainId'])
            cluster['CreationTime'] = cluster['CreationTime'].isoformat()
            cluster['LastModifiedTime'] = cluster['LastModifiedTime'].isoformat()
            inventory_object = extract_common_info(
                arn, cluster, region, account_id, time_generated, account_name)
            inventory.append(inventory_object)
        save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
            str(stack()[0][3]), region, account_id, idx))
        next_token = response.get('NextToken', None)
        idx = idx + 1
        if not next_token:
            break


def list_sagemaker_notebook_instance(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function lists SageMaker notebook instances, extracts common information, and saves the
    data as a Parquet file.

    :param file_path: The `file_path` parameter in the `list_sagemaker_notebook_instance` function is
    the path where the inventory data will be saved as a Parquet file. It should be a string
    representing the file path where you want to store the inventory information
    :param session: The `session` parameter in the `list_sagemaker_notebook_instance` function is
    typically an instance of a boto3 session that is used to create a client for interacting with AWS
    services. It allows you to make API calls to Amazon SageMaker services in this case
    :param region: The `region` parameter in the `list_sagemaker_notebook_instance` function is used to
    specify the AWS region where the SageMaker notebook instances are located. SageMaker resources such
    as notebook instances are region-specific, so you need to provide the region name where you want to
    list and describe the
    :param time_generated: Time when the function is being executed
    :param account: The `account` parameter in the `list_sagemaker_notebook_instance` function seems to
    be a dictionary containing information about the account. It likely includes keys such as
    'account_id' and 'account_name'. This parameter is used to extract the account ID and account name
    for further processing within the
    """
    next_token = None
    idx = 0
    client = session.client(
        'sagemaker', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        inventory = []
        response = client.list_notebook_instances(
            NextToken=next_token) if next_token else client.list_notebook_instances()
        for resource in response.get('NotebookInstances', []):
            notebook = client.describe_notebook_instance(
                NotebookInstanceName=resource['NotebookInstanceName'])
            arn = notebook['NotebookInstanceArn']
            notebook['CreationTime'] = notebook['CreationTime'].isoformat()
            notebook['LastModifiedTime'] = notebook['LastModifiedTime'].isoformat()
            inventory_object = extract_common_info(
                arn, notebook, region, account_id, time_generated, account_name)
            inventory.append(inventory_object)
        save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
            str(stack()[0][3]), region, account_id, idx))
        next_token = response.get('NextToken', None)
        idx = idx + 1
        if not next_token:
            break


def list_sagemaker_endpoint(file_path, session, region, time_generated, account, boto_config):
    """
    The function `list_sagemaker_endpoint` retrieves and processes information about SageMaker
    endpoints, formatting the data and saving it to a Parquet file.

    :param file_path: The `file_path` parameter in the `list_sagemaker_endpoint` function is the path
    where the inventory data will be saved as a file. It is the location where the Parquet file
    containing the endpoint information will be stored
    :param session: The `session` parameter in the `list_sagemaker_endpoint` function is typically an
    instance of a boto3 session that allows you to create service clients for AWS services. It is used
    to create a client for the Amazon SageMaker service in the specified region
    :param region: The `region` parameter in the `list_sagemaker_endpoint` function is used to specify
    the AWS region where the SageMaker endpoints are located. SageMaker is an AWS service for building,
    training, and deploying machine learning models. The region parameter is important because AWS
    resources, including SageMaker endpoints
    :param time_generated: The `time_generated` parameter in the `list_sagemaker_endpoint` function is
    used to specify the timestamp or time at which the inventory of SageMaker endpoints is being
    generated. This timestamp is typically used for tracking and auditing purposes to know when the
    inventory data was collected or generated. It helps in
    :param account: The `account` parameter in the `list_sagemaker_endpoint` function seems to be a
    dictionary containing information about the account. It likely includes keys such as 'account_id'
    and 'account_name'
    """
    next_token = None
    idx = 0
    client = session.client(
        'sagemaker', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        inventory = []
        response = client.list_endpoints(
            NextToken=next_token) if next_token else client.list_endpoints()
        for resource in response.get('Endpoints', []):
            endpoint = client.describe_endpoint(
                EndpointName=resource['EndpointName'])
            arn = endpoint['EndpointArn']
            endpoint['CreationTime'] = endpoint['CreationTime'].isoformat()
            endpoint['LastModifiedTime'] = endpoint['LastModifiedTime'].isoformat()
            pv_idx = 0
            spv_idx = 0
            for ProductionVariant in endpoint.get('ProductionVariants', []):
                di_idx = 0
                vs_idx = 0
                for DeployedImage in ProductionVariant.get('DeployedImages', []):
                    if 'ResolutionTime' in DeployedImage:
                        endpoint['ProductionVariants'][pv_idx]['DeployedImages'][di_idx][
                            'ResolutionTime'] = DeployedImage['ResolutionTime'].isoformat()
                    di_idx = di_idx + 1
                for VariantStatus in ProductionVariant.get('VariantStatus', []):
                    if 'StartTime' in VariantStatus:
                        endpoint['ProductionVariants'][pv_idx]['VariantStatus'][vs_idx][
                            'StartTime'] = VariantStatus['StartTime'].isoformat()
                    vs_idx = vs_idx + 1
                pv_idx = pv_idx + 1
            if 'PendingDeploymentSummary' in endpoint:
                pv_idx = 0
                spv_idx = 0
                for ProductionVariant in endpoint['PendingDeploymentSummary'].get('ProductionVariants', []):
                    di_idx = 0
                    vs_idx = 0
                    for DeployedImage in ProductionVariant.get('DeployedImages', []):
                        if 'ResolutionTime' in DeployedImage:
                            endpoint['PendingDeploymentSummary']['ProductionVariants'][pv_idx]['DeployedImages'][di_idx][
                                'ResolutionTime'] = DeployedImage['ResolutionTime'].isoformat()
                        di_idx = di_idx + 1
                    for VariantStatus in ProductionVariant.get('VariantStatus', []):
                        if 'StartTime' in VariantStatus:
                            endpoint['PendingDeploymentSummary']['ProductionVariants'][pv_idx]['VariantStatus'][vs_idx][
                                'StartTime'] = VariantStatus['StartTime'].isoformat()
                        vs_idx = vs_idx + 1
                    pv_idx = pv_idx + 1
                if 'StartTime' in endpoint['PendingDeploymentSummary']:
                    endpoint['PendingDeploymentSummary']['StartTime'] = endpoint['PendingDeploymentSummary']['StartTime'].isoformat()
                for ProductionVariant in endpoint['PendingDeploymentSummary'].get('ShadowProductionVariants', []):
                    di_idx = 0
                    vs_idx = 0
                    for DeployedImage in ProductionVariant.get('DeployedImages', []):
                        if 'ResolutionTime' in DeployedImage:
                            endpoint['PendingDeploymentSummary']['ShadowProductionVariants'][spv_idx]['DeployedImages'][di_idx][
                                'ResolutionTime'] = DeployedImage['ResolutionTime'].isoformat()
                        di_idx = di_idx + 1
                    for VariantStatus in ProductionVariant.get('VariantStatus', []):
                        if 'StartTime' in VariantStatus:
                            endpoint['PendingDeploymentSummary']['ShadowProductionVariants'][spv_idx]['VariantStatus'][vs_idx][
                                'StartTime'] = VariantStatus['StartTime'].isoformat()
                        vs_idx = vs_idx + 1
                    spv_idx = spv_idx + 1

            for ShadowProductionVariant in endpoint.get('ShadowProductionVariants', []):
                di_idx = 0
                vs_idx = 0
                for DeployedImage in ShadowProductionVariant.get('DeployedImages', []):
                    if 'ResolutionTime' in DeployedImage:
                        endpoint['ShadowProductionVariants'][spv_idx]['DeployedImages'][di_idx][
                            'ResolutionTime'] = DeployedImage['ResolutionTime'].isoformat()
                    di_idx = di_idx + 1
                for VariantStatus in ShadowProductionVariant.get('VariantStatus', []):
                    if 'StartTime' in VariantStatus:
                        endpoint['ShadowProductionVariants'][spv_idx]['VariantStatus'][vs_idx][
                            'StartTime'] = VariantStatus['StartTime'].isoformat()
                    vs_idx = vs_idx + 1
                spv_idx = spv_idx + 1
            inventory_object = extract_common_info(
                arn, endpoint, region, account_id, time_generated, account_name)
            inventory.append(inventory_object)
        save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
            str(stack()[0][3]), region, account_id, idx))
        next_token = response.get('NextToken', None)
        idx = idx + 1
        if not next_token:
            break


def list_sagemaker_models(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function lists SageMaker models, retrieves their details, and saves the information in a
    Parquet file.

    :param file_path: The `file_path` parameter in the `list_sagemaker_models` function is the path
    where the inventory data will be saved as a file. It should be a string representing the file path
    where you want to save the inventory data
    :param session: The `session` parameter in the `list_sagemaker_models` function is typically an
    instance of a boto3 session that is used to create a client for interacting with AWS services. It
    allows you to make API calls to Amazon SageMaker in this case
    :param region: The `region` parameter in the `list_sagemaker_models` function is used to specify the
    AWS region where the SageMaker models are located. SageMaker is an AWS service, and resources like
    models are region-specific. You need to provide the region name where your SageMaker models are
    deployed so
    :param time_generated: Time when the inventory is generated
    :param account: The `account` parameter in the `list_sagemaker_models` function seems to be a
    dictionary containing information about the account. It likely includes keys such as 'account_id'
    and 'account_name' which are used within the function to extract specific details for processing
    SageMaker models
    """
    next_token = None
    idx = 0
    client = session.client(
        'sagemaker', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        inventory = []
        response = client.list_models(
            NextToken=next_token) if next_token else client.list_models()
        for resource in response.get('Models', []):
            model = client.describe_notebook_instance(
                ModelName=resource['ModelName'])
            arn = model['ModelArn']
            model['CreationTime'] = model['CreationTime'].isoformat()
            inventory_object = extract_common_info(
                arn, model, region, account_id, time_generated, account_name)
            inventory.append(inventory_object)
        save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
            str(stack()[0][3]), region, account_id, idx))
        next_token = response.get('NextToken', None)
        idx = idx + 1
        if not next_token:
            break


def list_sagemaker_training_jobs(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function lists SageMaker training jobs, retrieves detailed information for each job, and
    saves the data in Parquet format.

    :param file_path: The `file_path` parameter in the `list_sagemaker_training_jobs` function is the
    path where the output file will be saved. This should be a valid file path on your system where you
    want to store the data retrieved from SageMaker training jobs
    :param session: The `session` parameter in the `list_sagemaker_training_jobs` function is typically
    an instance of a boto3 session that allows you to create service clients for AWS services. It is
    used to create a client for the Amazon SageMaker service in the specified region. This client is
    then used to
    :param region: The `region` parameter in the `list_sagemaker_training_jobs` function refers to the
    AWS region where the Amazon SageMaker training jobs are located. This parameter is used to specify
    the region when creating a client for the SageMaker service and when extracting information about
    training jobs in that specific region
    :param time_generated: The `time_generated` parameter in the `list_sagemaker_training_jobs` function
    is used to specify the timestamp when the inventory data is generated. This timestamp is typically
    used for tracking and logging purposes to indicate when the inventory information was collected or
    processed. It helps in maintaining a record of when the
    :param account: The `account` parameter in the `list_sagemaker_training_jobs` function seems to be a
    dictionary containing information about the account. It likely includes keys such as 'account_id'
    and 'account_name' which are used within the function to extract specific details for processing
    SageMaker training jobs
    """
    next_token = None
    idx = 0
    client = session.client(
        'sagemaker', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        inventory = []
        response = client.list_training_jobs(
            NextToken=next_token) if next_token else client.list_training_jobs()
        for resource in response.get('TrainingJobSummaries', []):
            training_job = client.describe_training_job(
                TrainingJobName=resource['TrainingJobName'])
            arn = training_job['TrainingJobArn']
            training_job['CreationTime'] = training_job['CreationTime'].isoformat()
            training_job['TrainingStartTime'] = training_job['TrainingStartTime'].isoformat(
            )
            training_job['TrainingEndTime'] = training_job['TrainingEndTime'].isoformat()
            training_job['LastModifiedTime'] = training_job['LastModifiedTime'].isoformat(
            )
            sst_idx = 0
            for SecondaryStatusTransition in training_job.get('SecondaryStatusTransitions', []):
                if 'StartTime' in SecondaryStatusTransition:
                    training_job['SecondaryStatusTransitions'][sst_idx]['StartTime'] = SecondaryStatusTransition['StartTime'].isoformat(
                    )
                if 'EndTime' in SecondaryStatusTransition:
                    training_job['SecondaryStatusTransitions'][sst_idx]['EndTime'] = SecondaryStatusTransition['EndTime'].isoformat(
                    )
                sst_idx = sst_idx + 1
            fmdl_idx = 0
            for FinalMetricDataList in training_job.get('FinalMetricDataList', []):
                if 'Timestamp' in FinalMetricDataList:
                    training_job['FinalMetricDataList'][fmdl_idx]['Timestamp'] = FinalMetricDataList['Timestamp'].isoformat(
                    )
                fmdl_idx = fmdl_idx + 1
            dres_idx = 0
            for DebugRuleEvaluationStatus in training_job.get('DebugRuleEvaluationStatuses', []):
                if 'LastModifiedTime' in DebugRuleEvaluationStatus:
                    training_job['DebugRuleEvaluationStatuses'][dres_idx]['LastModifiedTime'] = DebugRuleEvaluationStatus['LastModifiedTime'].isoformat(
                    )
                dres_idx = dres_idx + 1
            pres_idx = 0
            for ProfilerRuleEvaluationStatus in training_job.get('ProfilerRuleEvaluationStatuses', []):
                if 'LastModifiedTime' in ProfilerRuleEvaluationStatus:
                    training_job['ProfilerRuleEvaluationStatuses'][pres_idx]['LastModifiedTime'] = ProfilerRuleEvaluationStatus['LastModifiedTime'].isoformat(
                    )
                pres_idx = pres_idx + 1
            inventory_object = extract_common_info(
                arn, training_job, region, account_id, time_generated, account_name)
            inventory.append(inventory_object)
        save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
            str(stack()[0][3]), region, account_id, idx))
        next_token = response.get('NextToken', None)
        idx = idx + 1
        if not next_token:
            break


def list_sagemaker_hpt_jobs(file_path, session, region, time_generated, account, boto_config):
    """
    The function `list_sagemaker_hpt_jobs` retrieves and processes information about SageMaker
    hyperparameter tuning jobs.

    :param file_path: The `file_path` parameter in the `list_sagemaker_hpt_jobs` function is the path
    where you want to save the output file containing information about SageMaker Hyperparameter Tuning
    Jobs. This file will be saved in Parquet format
    :param session: The `session` parameter in the `list_sagemaker_hpt_jobs` function is typically an
    instance of a boto3 session that allows you to create service clients for AWS services. It is used
    to create a client for the Amazon SageMaker service in the specified region. This client is then
    used
    :param region: The `region` parameter in the `list_sagemaker_hpt_jobs` function is used to specify
    the AWS region where the SageMaker service is located. SageMaker resources such as hyperparameter
    tuning jobs are region-specific, so you need to provide the region where these resources are located
    in order to
    :param time_generated: The `time_generated` parameter in the `list_sagemaker_hpt_jobs` function is
    used to specify the timestamp or time at which the inventory of SageMaker Hyperparameter Tuning jobs
    is generated. This timestamp is typically used for tracking and auditing purposes to indicate when
    the inventory data was collected or
    :param account: The `list_sagemaker_hpt_jobs` function you provided seems to be listing SageMaker
    Hyperparameter Tuning jobs and saving the information to a Parquet file. It iterates through the
    list of Hyperparameter Tuning jobs, extracts relevant information, and saves it to a Parquet file
    """
    next_token = None
    idx = 0
    client = session.client(
        'sagemaker', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        inventory = []
        response = client.list_hyper_parameter_tuning_jobs(
            NextToken=next_token) if next_token else client.list_hyper_parameter_tuning_jobs()
        for resource in response.get('HyperParameterTuningJobSummaries', []):
            hpt_job = client.describe_hyper_parameter_tuning_job(
                HyperParameterTuningJobName=resource['HyperParameterTuningJobName'])
            arn = hpt_job['HyperParameterTuningJobArn']
            hpt_job['CreationTime'] = hpt_job['CreationTime'].isoformat()
            hpt_job['LastModifiedTime'] = hpt_job['LastModifiedTime'].isoformat(
            )
            hpt_job['HyperParameterTuningEndTime'] = hpt_job['HyperParameterTuningEndTime'].isoformat()
            if 'BestTrainingJob' in hpt_job:
                if 'CreationTime' in hpt_job.get('BestTrainingJob', {}):
                    hpt_job['BestTrainingJob']['CreationTime'] = hpt_job['BestTrainingJob']['CreationTime'].isoformat()
                if 'TrainingStartTime' in hpt_job.get('BestTrainingJob', {}):
                    hpt_job['BestTrainingJob']['TrainingStartTime'] = hpt_job['BestTrainingJob']['TrainingStartTime'].isoformat()
                if 'TrainingEndTime' in hpt_job.get('BestTrainingJob', {}):
                    hpt_job['BestTrainingJob']['TrainingEndTime'] = hpt_job['BestTrainingJob']['TrainingEndTime'].isoformat()
            if 'OverallBestTrainingJob' in hpt_job:
                if 'CreationTime' in hpt_job.get('OverallBestTrainingJob', {}):
                    hpt_job['OverallBestTrainingJob']['CreationTime'] = hpt_job['OverallBestTrainingJob']['CreationTime'].isoformat()
                if 'TrainingStartTime' in hpt_job.get('OverallBestTrainingJob', {}):
                    hpt_job['OverallBestTrainingJob']['TrainingStartTime'] = hpt_job['OverallBestTrainingJob']['TrainingStartTime'].isoformat()
                if 'TrainingEndTime' in hpt_job.get('OverallBestTrainingJob', {}):
                    hpt_job['OverallBestTrainingJob']['TrainingEndTime'] = hpt_job['OverallBestTrainingJob']['TrainingEndTime'].isoformat()
            if 'TuningJobCompletionDetails' in hpt_job:
                if 'ConvergenceDetectedTime' in hpt_job.get('TuningJobCompletionDetails', {}):
                    hpt_job['TuningJobCompletionDetails']['ConvergenceDetectedTime'] = hpt_job[
                        'TuningJobCompletionDetails']['ConvergenceDetectedTime'].isoformat()
            inventory_object = extract_common_info(
                arn, hpt_job, region, account_id, time_generated, account_name)
            inventory.append(inventory_object)
        save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
            str(stack()[0][3]), region, account_id, idx))
        next_token = response.get('NextToken', None)
        idx = idx + 1
        if not next_token:
            break
