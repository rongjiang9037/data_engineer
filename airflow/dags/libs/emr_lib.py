import boto3
import json
import requests
import time
import logging

from airflow.models import Variable



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
    return cluster_id["JobFlowId"]


def terminate_emr_cluster():
    """
    This function terminate the EMR job flow.

    :return - cluster id
    """
    ## get variables
    EMR_REGION = Variable.get("EMR_REGION")
    AWS_KEY = Variable.get("AWS_KEY")
    AWS_SECRET = Variable.get("AWS_SECRET")
    EMR_NAME = Variable.get("EMR_NAME")

    ## create EMR client object
    emr_client = boto3.client("emr",
                             region_name=EMR_REGION,
                             aws_access_key_id= AWS_KEY,
                             aws_secret_access_key= AWS_SECRET)

    ## get all running & waiting clusters
    clusters_reponse = emr_client.list_clusters(
    ClusterStates=["RUNNING", "WAITING"])

    ## loop over clusters
    for cluster in clusters_reponse["Clusters"]:
        if cluster["Name"] == EMR_NAME:
            cluster_id = cluster["Id"]
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
    while r["state"] != "idle":
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


