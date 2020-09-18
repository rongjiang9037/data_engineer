import boto3
import json
import requests
import time
import logging

from airflow.models import Variable
from airflow import AirflowException


def get_emr_sg_id(ec2_client, VPC_NAME):
    """
    This function retrieves security group id of the EMR master node.

    :param ec2_client: ec2 client object
    :param VPC_NAME: VPC name
    :return:
    """
    ## 1. get VPC id
    vpc = ec2_client.describe_vpcs(Filters=[{"Name": "tag:Name",
                                      "Values":[VPC_NAME]}])
    vpc_id = vpc["Vpcs"][0]["VpcId"]

    ## 2. get security group ids from this VPC
    sg = ec2_client.describe_security_groups(Filters=[{"Name": "vpc-id",
                                                    "Values": vpc_id}
                                                 ])

    ## 3. loop over all sg and get security group id for the default and EMR master node
    for sg_ in sg["SecurityGroups"]:
        if sg_["GroupName"] == "ElasticMapReduce-master":
            emr_sg = sg_
    return emr_sg

def get_ec2_public_ip(ec2_client):
    """
    This function get public ip address of a EC2 instance.
    :param ec2_client: ec2 client object
    :return: ip add
    """
    ## get EC2_NAME
    EC2_NAME = Variable.get("EC2_NAME")
    ec2_response = ec2_client.describe_instances(Filters=[{"Name": "tag:Name",
                                      "Values":[EC2_NAME]}])
    try:
        ec2_public_ip = ec2_response["Reservations"][0]["Instances"][0]["NetworkInterfaces"][0]["Association"]["PublicIp"]
    except Exception as e:
        logging.info(e)
        raise AirflowException("EC2 instnce {} haven't created yet.".format(EC2_NAME))
    return ec2_public_ip


def check_access_status(emr_sg, ec2_public_ip):
    """
    This function check if airflow server has access to EMR master node.

    :param emr_sg: EMR master node security group
    :param ec2_public_ip: public ip of the EC2 instance
    :return:
    """

    already_have_access = False
    for permission_sg in emr_sg["IpPermissions"]:
        if permission_sg["FromPort"] == 8998 and permission_sg["ToPort"] == 8998:
            cidrip = permission_sg["IpRanges"][0]["CidrIp"]
            if cidrip == ec2_public_ip + "/32":
                already_have_access = True
                break
    return already_have_access

def add_access(ec2_client, emr_sg_id, ec2_public_ip):
    """
    This function add inbound rule to EMR sg, opeing port 8998 to EC2 public ip

    :param ec2_client: EC2 client object
    :param emr_sg_id: EMR master node security group ID
    :param ec2_public_ip: public ip of the EC2 instance
    :return:
    """
    ec2_client.authorize_security_group_ingress(GroupId = emr_sg_id,
                                        IpPermissions=[
                                        {
                                            "FromPort": 8998,
                                            "IpProtocol": "tcp",
                                            "IpRanges": [
                                                {
                                                    "CidrIp": ec2_public_ip + "/32",
                                                },
                                            ],
                                            "ToPort": 8998,
                                        }
                                    ],)



def open_livy_port_to_airflow(ec2_client):
    """
    This function add security rule to master"s node.
    When a new EMR cluster is created, the master node is not configured to be accessed from
    outside of the cluster.
    If the EC2 instance that runs airflow server needs to access it,
    we need to manually edit the security group.
    port number: 8998
    group id: security group id of the EC2 instance.

    :param ec2_client: ec2 client object
    :param VPC_NAME: the name of the VPC object
    :return:
    """
    VPC_NAME = Variable.get("VPC_NAME")

    ## get security id of a the EMR master node
    emr_sg = get_emr_sg_id(ec2_client, VPC_NAME)
    emr_sg_id = emr_sg['GroupId']

    ## get ec2 public ip
    ec2_public_ip = get_ec2_public_ip(ec2_client)

    ## check access status
    already_have_access = check_access_status(emr_sg, ec2_public_ip)


    if not already_have_access:
        add_access(ec2_client, emr_sg_id, ec2_public_ip)



def create_emr_cluster():
    """
    This function create a EMR cluster and return cluster ID.
    :return -  cluster id
    """
    ## get variables
    EMR_REGION = Variable.get("EMR_REGION")
    AWS_KEY = Variable.get("AWS_KEY")
    AWS_SECRET = Variable.get("AWS_SECRET")
    PUB_SUBNET_NAME = Variable.get("PUB_SUBNET_NAME")
    EMR_NAME = Variable.get("EMR_NAME")
    S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")
    EMR_MASTER_NAME = Variable.get("EMR_MASTER_NAME")
    EMR_WORKER_NAME = Variable.get("EMR_WORKER_NAME")
    EMR_TYPE = Variable.get("EMR_TYPE")
    MASTER_COUNT = int(Variable.get("MASTER_COUNT"))
    WORKER_COUNT = int(Variable.get("WORKER_COUNT"))

    ## create EMR client object
    emr_client = boto3.client("emr",
                             region_name=EMR_REGION,
                             aws_access_key_id=AWS_KEY,
                             aws_secret_access_key=AWS_SECRET)

    ## get public subnet ID
    ec2_client = boto3.client("ec2",
                     region_name=EMR_REGION,
                     aws_access_key_id=AWS_KEY,
                     aws_secret_access_key= AWS_SECRET)

    pub_subnet = ec2_client.describe_subnets(Filters=[{"Name": "tag:Name",
                                      "Values":[PUB_SUBNET_NAME]}])
    pub_subnet_id = pub_subnet["Subnets"][0]["SubnetId"]

    ## create EMR cluster
    cluster_id = emr_client.run_job_flow(
        Name=EMR_NAME,
        LogUri=f"s3://{S3_BUCKET_NAME}/logs/",
        Instances={
            "InstanceGroups":[{
                "Name": EMR_MASTER_NAME,
                "Market":"ON_DEMAND",
                "InstanceRole":"MASTER",
                "InstanceType":EMR_TYPE,
                "InstanceCount":MASTER_COUNT,
            },
            {   "Name": EMR_WORKER_NAME,
                "Market":"ON_DEMAND",
                "InstanceRole":"CORE",
                "InstanceType":EMR_TYPE,
                "InstanceCount":WORKER_COUNT,
            }],
            "Ec2SubnetId":pub_subnet_id,
            "KeepJobFlowAliveWhenNoSteps":True
        },
        Applications=[
            {   "Name":"Spark"},
            {   "Name":"Hadoop"},
            {   "Name": "livy" },
        ],
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
        ReleaseLabel="emr-5.28.0",
    )

    ## wait till the cluster is ready
    waiter = emr_client.get_waiter("cluster_running")
    waiter.wait(
        ClusterId=cluster_id["JobFlowId"],
    )

    ## edit EMR"s security group to allow access from the EC2 instance
    ## that runs the airflow
    open_livy_port_to_airflow(ec2_client)

    return cluster_id["JobFlowId"]

def get_emr_cluster_id(emr_client):
    """
    This function get EMR cluster id.

    :param emr_client: EMR client object
    :return:
    """
    EMR_NAME = Variable.get("EMR_NAME")
    ## get all running & waiting clusters
    clusters_reponse = emr_client.list_clusters(
    ClusterStates=["RUNNING", "WAITING"])

    ## loop over clusters
    for cluster in clusters_reponse["Clusters"]:
        if cluster["Name"] == EMR_NAME:
            cluster_id = cluster["Id"]
            break
    return cluster_id



def terminate_emr_cluster():
    """
    This function terminate the EMR job flow.

    :return - cluster id
    """
    ## get variables
    EMR_REGION = Variable.get("EMR_REGION")
    AWS_KEY = Variable.get("AWS_KEY")
    AWS_SECRET = Variable.get("AWS_SECRET")

    ## create EMR client object
    emr_client = boto3.client("emr",
                             region_name=EMR_REGION,
                             aws_access_key_id= AWS_KEY,
                             aws_secret_access_key= AWS_SECRET)

    cluster_id = get_emr_cluster_id(emr_client)

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

def get_master_dns(cluster_id):
    """
    This function get muster node"s public DNS.

    :param cluster_id:
    :return:
    """
    ## get variables
    EMR_REGION = Variable.get("EMR_REGION")
    AWS_KEY = Variable.get("AWS_KEY")
    AWS_SECRET = Variable.get("AWS_SECRET")
    ## get EMR client object
    emr_client = boto3.client("emr",
                         region_name=EMR_REGION,
                         aws_access_key_id=AWS_KEY,
                         aws_secret_access_key= AWS_SECRET)

    response = emr_client.describe_cluster(ClusterId=cluster_id)
    return response["Cluster"]["MasterPublicDnsName"]

def create_spark_session(cluster_dns):
    """
    This function create a spark session on EMR master node.
    :param cluster_dns:
    :return:
    """
    ## parameters to start session
    host = "http://{}:8998".format(cluster_dns)
    headers = {"Content-Type": "application/json"}
    data  = {"kind": "pyspark"}

    ## start session
    r = requests.post(host+"/sessions", data=json.dumps(data), headers=headers)
    session_url = host + r.headers["location"]

    ## wait till session is ready
    while r.json()["state"] != "idle":
        r = requests.get(session_url, headers=headers)
        time.sleep(5)
    return session_url

def submit_statement(session_url, cluster_dns, code_path, params_key):
    """
    This function submit statement to EMR spark session via Livy.

    :param session_url: idle session url
    :param code_path:
    :return:
    """
    ## get code
    with open(code_path, "r") as f:
        codes = f.readlines()
    codes = "".join(codes)

    ## get params
    host = "http://{}:8998".format(cluster_dns)
    headers = {"Content-Type": "application/json"}
    data = {"code": codes.format(**params_key)}

    ## post statements to EMR cluster
    r = requests.post(session_url+"/statements", data=json.dumps(data), headers=headers)
    statement_url = host+r.headers["location"]
    return statement_url


def track_statement_progress(statement_url):
    """
    This function track run log.

    :param statement_url: statment url
    :param cluter_dns: EMR mater DNS
    :return: run log
    """
    ## prepare parameteter
    headers = {"Content-Type": "application/json"}

    ## check running status
    r = requests.get(statement_url, headers=headers)

    ## while till the run finishes
    while r["output"]["status"] == "running":
        time.sleep(3)
        r = requests.get(statement_url, headers=headers)
    return r["output"]


def kill_spark_session(session_url):
    """
    This function kills Spark Cluster session.

    :param session_url: Cluster session url
    :return:
    """
    ## prepare parameteter
    headers = {"Content-Type": "application/json"}

    requests.delete(session_url, headers=headers)


