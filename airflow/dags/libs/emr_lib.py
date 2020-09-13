import boto3
import configparser

from airflow.models import Variable



def create_emr_cluster():
    """
    This function create a EMR cluster and return cluster ID.
    :return -  cluster id
    """
    ## get variables
    EMR_REGION = Variable.get('EMR_REGION')
    AWS_KEY = Variable.get('AWS_KEY')
    AWS_SECRET = Variable.get('AWS_SECRET')
    PUB_SUBNET_NAME = Variable.get('PUB_SUBNET_NAME')
    EMR_NAME = Variable.get('EMR_NAME')
    S3_BUCKET_NAME = Variable.get('S3_BUCKET_NAME')
    EMR_MASTER_NAME = Variable.get('EMR_MASTER_NAME')
    EMR_WORKER_NAME = Variable.get('EMR_WORKER_NAME')
    EMR_TYPE = Variable.get('EMR_TYPE')
    MASTER_COUNT = int(Variable.get('MASTER_COUNT'))
    WORKER_COUNT = int(Variable.get('WORKER_COUNT'))

    ## create EMR client object
    emr_client = boto3.client('emr',
                             region_name=EMR_REGION,
                             aws_access_key_id=AWS_KEY,
                             aws_secret_access_key=AWS_SECRET)

    ## get public subnet ID
    ec2_client = boto3.client('ec2',
                     aws_access_key_id=AWS_KEY,
                     aws_secret_access_key= AWS_SECRET)

    pub_subnet = ec2_client.describe_subnets(Filters=[{'Name': 'tag:Name',
                                      'Values':[PUB_SUBNET_NAME]}])
    pub_subnet_id = pub_subnet['Subnets'][0]['SubnetId']

    ## create EMR cluster
    cluster_id = emr_client.run_job_flow(
        Name=EMR_NAME,
        LogUri=f"s3://{S3_BUCKET_NAME}/logs/",
        Instances={
            'InstanceGroups':[{
                'Name': EMR_MASTER_NAME,
                'Market':'ON_DEMAND',
                'InstanceRole':'MASTER',
                'InstanceType':EMR_TYPE,
                'InstanceCount':MASTER_COUNT,
            },
            {   'Name': EMR_WORKER_NAME,
                'Market':'ON_DEMAND',
                'InstanceRole':'CORE',
                'InstanceType':EMR_TYPE,
                'InstanceCount':WORKER_COUNT,
            }],
            'Ec2SubnetId':pub_subnet_id,
            'KeepJobFlowAliveWhenNoSteps':True
        },
        Applications=[
            {   'Name':'Spark'},
            {   'Name':'Hadoop'},
            {   'Name': 'livy' },
        ],
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        ReleaseLabel='emr-5.28.0',
    )

    ## wait till the cluster is ready
    waiter = emr_client.get_waiter("cluster_running")
    waiter.wait(
        ClusterId=cluster_id['JobFlowId'],
    )
    return cluster_id['JobFlowId']


def terminate_emr_cluster():
    """
    This function terminate the EMR job flow.

    :return - cluster id
    """
    ## get variables
    EMR_REGION = Variable.get('EMR_REGION')
    AWS_KEY = Variable.get('AWS_KEY')
    AWS_SECRET = Variable.get('AWS_SECRET')
    EMR_NAME = Variable.get('EMR_NAME')

    ## create EMR client object
    emr_client = boto3.client('emr',
                             region_name=EMR_REGION,
                             aws_access_key_id= AWS_KEY,
                             aws_secret_access_key= AWS_SECRET)

    ## get all running & waiting clusters
    clusters_reponse = emr_client.list_clusters(
    ClusterStates=['RUNNING', 'WAITING'])

    ## loop over clusters
    for cluster in clusters_reponse['Clusters']:
        if cluster['Name'] == EMR_NAME:
            cluster_id = cluster['Id']
            break

    ## terminate job flow
    emr_client.terminate_job_flows(
        JobFlowIds=[cluster_id]
    )
    ## wait till job flow terminated
    waiter = emr_client.get_waiter("cluster_terminated")
    waiter.wait(
        ClusterId=cluster_id,
    )
    return cluster_id
