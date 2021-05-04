import configparser
import boto3
import json
import argparse

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
KEY = config.get('AWS','KEY')
SECRET = config.get('AWS','SECRET')
DWH_CLUSTER_TYPE = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH","DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("DWH","DWH_DB")
DWH_DB_USER = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH","DWH_PORT")
DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")


def create_cluster(iam_role):
    """Create Redshift cluster on AWS."""
    redshift = boto3.client('redshift', region_name='us-east-1',
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET)
    try:
        redshift.create_cluster(        
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            IamRoles=[iam_role]
        )
    except Exception as e:
        print(e)
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    return (myClusterProps['VpcId'],
            myClusterProps['Endpoint']['Address'],
            myClusterProps['IamRoles'][0]['IamRoleArn'])

def delete_cluster():
    """Delete Redshift cluster on AWS."""
    redshift = boto3.client('redshift', region_name='us-east-1',
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET)
    try:
        redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
                                SkipFinalClusterSnapshot=True)
    except Exception as e:
        print(e)
    return
    
def create_iam():
    """Create IAM role on AWS."""
    iam = boto3.client('iam', region_name='us-east-1', aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET)
    try:
        iam.create_role(Path='/',
                        RoleName=DWH_IAM_ROLE_NAME,
                        AssumeRolePolicyDocument=json.dumps(
                        {'Statement': [{'Action': 'sts:AssumeRole',
                                       'Effect': 'Allow',
                                       'Principal': {'Service': 'redshift.amazonaws.com'}}],
                        'Version': '2012-10-17'}),
                        Description='Allow Redshift to access S3 bucket',
                    )
    except Exception as e:
        print(e)
    try:
        iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                              PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                              )['ResponseMetadata']['HTTPStatusCode']
    except Exception as e:
        print(e)
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)
    return roleArn['Role']['Arn']

def delete_iam():
    """Delete IAM role on AWS."""
    iam = boto3.client('iam', region_name='us-east-1', aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET)
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    return

def create_vpc(vpc_id):
    """Create VPC on AWS."""
    ec2 = boto3.resource('ec2', region_name='us-east-1', aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET)
    try:
        vpc = ec2.Vpc(id=vpc_id)
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        
        defaultSg.authorize_ingress(
            GroupName= defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)
    return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--delete', default=argparse.SUPPRESS)
    args = parser.parse_args()
    if "delete" in args:
        delete_cluster()
        delete_iam()
    else:
        iam_role = create_iam()
        vpc_id, endpoint, role_arn = create_cluster(iam_role)
        create_vpc(vpc_id)
    
if __name__ == "__main__":
    main()