import os

import boto3
import configparser

from botocore.exceptions import ClientError

config_aws = configparser.ConfigParser()
config_aws.read_file(open('../config/aws_credentials.cfg'))

KEY                    = config_aws.get('AWS','KEY')
SECRET                 = config_aws.get('AWS','SECRET')
ARN                    = config_aws.get('AWS', 'ARN')

config_instance = configparser.ConfigParser()
config_instance.read_file(open('../config/aws_setup.cfg'))

AMI_ID                 = config_instance.get('AWS', 'AMI_ID')
KEY_PAIR_NAME          = config_instance.get('AWS', 'KEY_PAIR_NAME') 
EC2_REGION             = config_instance.get('EC2', 'REGION')
EC2_TYPE               = config_instance.get('EC2', 'INSTANCE_TYPE')
EC2_NAME               = config_instance.get('EC2', 'NAME')
VPC_NAME               = config_instance.get('VPC', 'NAME')
VPC_CIDR_BLOCK         = config_instance.get('VPC', 'CIDR_BLOCK')
PUB_SUBNET_NAME           = config_instance.get('VPC', 'PUB_SUBNET_NAME')
PUB_SUBNET_CIDR_BLOCK     = config_instance.get('VPC', 'PUB_SUBNET_CIDR_BLOCK')
PUB_SUBNET_REGION         = config_instance.get('VPC', 'PUB_SUBNET_REGION')
PVR_SUBNET_NAME           = config_instance.get('VPC', 'PVR_SUBNET_NAME')
PVR_SUBNET_CIDR_BLOCK     = config_instance.get('VPC', 'PVR_SUBNET_CIDR_BLOCK')
PVR_SUBNET_REGION         = config_instance.get('VPC', 'PVR_SUBNET_REGION')
RT_NAME                = config_instance.get('ROUTE_TABLE', 'NAME')
IG_NAME                = config_instance.get('IG', 'NAME')

def get_keypair(ec2):
    """
    This function create key pair for the new EC instance.
    
    input:
    ec2 -- ec2 resource object
    """
    # call the boto ec2 function to create a key pair
    key_pair = ec2.create_key_pair(KeyName=KEY_PAIR_NAME)
    print("\n===Created a new key pair in AWS.")

    # capture the key and store it in a file
    KeyPairOut = str(key_pair.key_material)

    # create a file to store the key locally
    print("Saving the keypair.")
    key_pair_path = KEY_PAIR_NAME + '.pem'
    with open(key_pair_path, 'w') as f:
        f.write(KeyPairOut)
    os.chmod(key_pair_path, 0o600)
    print("===Changed access permission to read-only.")
    
    
def create_vpc(ec2):
    """
    This function create a new VPC.
    
    input:
    ec2 -- ec2 resource object.
    """
    # create a new VPC
    print("\n===Creating VPC...")
    vpc = ec2.create_vpc(CidrBlock=VPC_CIDR_BLOCK,
                         TagSpecifications=[{'ResourceType': 'vpc',
                                             'Tags':[{"Key": "Name", 
                                                      "Value": VPC_NAME},
                                                    ]
                                            }])
    
    # wait till available and return VPC ID
    vpc.wait_until_available()
    print(f"===VPC {VPC_NAME} is available!")
    return vpc


def create_subnet(ec2, vpc, 
                  subnet_name,
                  subnet_region, 
                  subnet_cidr_block,
                  subnet_type='private'):
    """
    This function creates a new subnet.
    
    input:
    ec2 -- ec2 resource object
    vpc -- the vpc where subnet resides in
    subnet_region -- region name
    subnet_type -- private / public. Public subnet can be accessed from outside of the VPC.
                   While creating new subnets, they are initially to be all private.
                   Public subnet can be configured by editing its route table in function ()
    """
    # create a public subnet within the VPC
    print("\n===Creating a "+subnet_type+" subnet...")
    subnet = ec2.create_subnet(
        AvailabilityZone=subnet_region,
        CidrBlock=subnet_cidr_block,
        VpcId=vpc.vpc_id,
        DryRun=False,
        TagSpecifications=[{
            'ResourceType':'subnet',
            'Tags':[{"Key": "Name", "Value": subnet_name},
                   ]
        }])
    
    print(f"===Subnet {subnet_name} is available!")
    return subnet


def stop_instance(ec2_client, instances):
    """
    This function stops the ec2 instances.
    
    input:
    ec2_client -- ec2_client object
    instances -- a list of ec2 instance objects
    """
    # get a list of instance ids
    instances_ids = [i.instance_id for i in instances]
    
    # start the instances
    ec2_client.stop_instances(InstanceIds=instances_ids)
    
    # wait till instance is stopped
    waiter = ec2_client.get_waiter('instance_stopped')
    waiter.wait(InstanceIds=instances_ids)
    print("\n===EC2 instance has stopped!")
    
    
def start_instance(ec2_client, instances):
    """
    This function starts the ec2 instances.
    
    input:
    ec2_client -- ec2_client object
    instances -- a list of ec2 instance objects
    """
    # get a list of instance ids
    instances_ids = [i.instance_id for i in instances]
    
    # start the instances
    print("\n===Creating EC2 instance.")
    ec2_client.start_instances(InstanceIds=instances_ids)
    
    # wait till instance is ready
    waiter = ec2_client.get_waiter('instance_running')
    waiter.wait(InstanceIds=instances_ids)
    print("===EC2 instance is ready!")
    
    
def create_ig(ec2):
    """
    This function creates internet gateway.
    
    input:
    ec2 -- ec2 resource object
    """
    ## create internet gateway
    print("\n===Creating Internet Gateway...")
    ig = ec2.create_internet_gateway(TagSpecifications=[{
            'ResourceType':'internet-gateway',
            'Tags':[{"Key": "Name", "Value": IG_NAME},
                   ]}])
    print("===Internet gateway is reay!!")
    return ig

def establish_connection(ec2_client, vpc, ig, subnet_pub):
    """
    This function establishes outside connection to EC2 instance.
    Firstly, it adds new route to route table of the subnet to make it public.
    Secondly, it add SSH connection to security group of the VPC.
    
    input:
    ec2_client -- ec2 client object
    vpc -- vpc object where public subnet and EC2 instance reside.
    ig -- internet gateway object
    subnet_pub -- public subnet object
    """
    ## 1.1 create a new route table for public subnet
    print("\n===Creating a new route table for public subnet allowing public traffic.")
    routetable = vpc.create_route_table(
        TagSpecifications=[
            {
                'ResourceType': 'route-table',
                'Tags': [
                    {
                        'Key': 'Name',
                        'Value': RT_NAME
                    },
                ]
            },
        ]
    )
    ## 1.2 create new route, that allows traffic outside of VPC to go to the internet gateway
    route = routetable.create_route(DestinationCidrBlock='0.0.0.0/0', GatewayId=ig.id)
    ## 1.3 attach the new route to the public subnet
    routetable.associate_with_subnet(SubnetId=subnet_pub.id)
    print("===Route table is ready.")
    
    
    ## 2.1 get default security group id
    print("\n===Config security group, allowing public traffic.")
    sg = ec2_client.describe_security_groups(Filters=[{'Name': 'vpc-id',
                                                        'Values': [vpc.vpc_id]}
                                                     ])
    sg_id = sg['SecurityGroups'][0]['GroupId']
    ## 2.2 add imbound rule for the security group
    ## allowing SSH and airflow connect from all the internet
    ec2_client.authorize_security_group_ingress(GroupId = sg_id,
                                            IpPermissions=[
                                            {
                                                'FromPort': 22,
                                                'IpProtocol': 'tcp',
                                                'IpRanges': [
                                                    {
                                                        'CidrIp': '0.0.0.0/0',
                                                        'Description': 'SSH access from outside',
                                                    },
                                                ],
                                                'ToPort': 22,
                                            },
                                            {
                                            'FromPort': 8080,
                                            'IpProtocol': 'tcp',
                                            'IpRanges': [
                                                {
                                                    'CidrIp': '0.0.0.0/0',
                                                    'Description': 'Apache Airflow',
                                                },
                                            ],
                                            'ToPort': 8080,
                                        }
                                        ],)
    print("===Security group config is ready.")
    
    
def create_ec2_with_eip(ec2, ec2_client, subnet_pub):
    """
    This function creates a new EC2 instance and give it a Elastic IP address.
    
    input:
    ec2 -- ec2 resouce object
    ec2_client -- ec2 client object
    subnet_pub -- public subnet object
    
    """
    ## create EC2 instance
    print("\n===Creating an EC2 instance")
    instances = ec2.create_instances(
         ImageId=AMI_ID,
         MinCount=1,
         MaxCount=1,
         InstanceType=EC2_TYPE,
         KeyName=KEY_PAIR_NAME,
         NetworkInterfaces=[{
                 'DeviceIndex':0,
                 'SubnetId': subnet_pub.subnet_id}],
         TagSpecifications=[{
                'ResourceType':'instance',
                'Tags':[{"Key": "Name", "Value": EC2_NAME}]
                }]
     )
    
    ## get instance ids
    instances_ids = [i.instance_id for i in instances]

    ## wait till instance is ready
    waiter = ec2_client.get_waiter('instance_running')
    waiter.wait(InstanceIds=instances_ids)
    print("An EC2 instance is ready.")

    ## create new EIP and attach it to existing EC2 instance
    instance_id = instances[0].instance_id
    try:
        allocation = ec2_client.allocate_address(Domain='vpc')
        response = ec2_client.associate_address(AllocationId=allocation['AllocationId'],
                                         InstanceId=instance_id)
        print(response)
    except ClientError as e:
        print(e)
    print(f"===EIP {allocation['PublicIp']} has been assigned to the EC2 instance!")
    return instances, allocation['PublicIp']




    
if __name__ == '__main__':
    """
    This script create AWS objects needed for the application.
    """
    ## create a ec2 resource instance
    ec2 = boto3.resource('ec2', 
                        region_name = EC2_REGION,
                        aws_access_key_id = KEY,
                        aws_secret_access_key = SECRET)
    
    ## create a ec2 client instnace
    ec2_client = boto3.client('ec2')
    
    ## create a key pair and download them 
    get_keypair(ec2)
    
    ## create a VPC instance
    vpc = create_vpc(ec2)
    
    ## create a public subnet
    subnet_pub = create_subnet(ec2=ec2, vpc=vpc, subnet_name=PUB_SUBNET_NAME,
                            subnet_region=PUB_SUBNET_REGION, 
                            subnet_cidr_block=PUB_SUBNET_CIDR_BLOCK,
                            subnet_type='public')
    ## create a private subnet
    subnet_prv = create_subnet(ec2=ec2, vpc=vpc, subnet_name=PVR_SUBNET_NAME,
                            subnet_region=PVR_SUBNET_REGION, 
                            subnet_cidr_block=PVR_SUBNET_CIDR_BLOCK,
                            subnet_type='private')
    
    ## create internet gateway
    ig = create_ig(ec2)
    ## attach the internet gateway to the VPC
    vpc.attach_internet_gateway(InternetGatewayId = ig.id)
    print("Attached the internet gateway to the VPC.")
    
    ## set up SSH connection from outside of VPC
    establish_connection(ec2_client, vpc, ig, subnet_pub)
    instances, public_ip = create_ec2_with_eip(ec2, ec2_client, subnet_pub)
    print("\n===EC instance is ready!")
    print(f"\nssh -i ec2-key.pem ec2-user@{public_ip}")
    
    
    
