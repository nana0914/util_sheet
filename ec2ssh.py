
# -*- coding: utf-8 -*-

import pdb

import boto
import boto3
import botocore
import paramiko
import pysnooper
import os
import configparser

# --------------------------------------------------
# configparserの宣言とiniファイルの読み込み
# --------------------------------------------------
conf = configparser.ConfigParser()
conf.read('config.ini', encoding='utf-8')

# --------------------------------------------------
# config,iniから値取得
# --------------------------------------------------

ACCESS_KEY = conf['AWS']['ACCESS_KEY']
SECRET_KEY = conf['AWS']['SECRET_KEY']
REGION_NAME = conf['AWS']['REGION_NAME']
PEM_KEY = conf['AWS']['PEM_KEY']


#paramiko log
os.makedirs('logs', exist_ok=True)
LOG_FILE='logs/p_log.txt'


paramiko.util.log_to_file(LOG_FILE)


# @pysnooper.snoop()
def get_ec2_client(region=REGION_NAME):
    ec2 = boto3.session.Session(
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY).client('ec2', region)
    return ec2


@pysnooper.snoop()
def get_ec2_resouce(region=REGION_NAME):
    ec2 = boto3.resource(
        service_name='ec2',
        region_name=region,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY)
    return ec2


# parse tags lists
def parse_sets(tags):
    result = {}
    for tag in tags:
        key = tag['Key']
        val = tag['Value']
        result[key] = val
    return result



# find instance with instance name
def find_ec2_instanceid(instance_name):
    ec2 = get_ec2_client()
    instances = ec2.describe_instances()
    instance_list = []
    for reservations in instances['Reservations']:
        for instance in reservations['Instances']:
            tags = parse_sets(instance['Tags'])
            if tags['Name'] == instance_name:
                return instance['InstanceId']


def ec2_return_public_ip(instance_name):
    instance_id = find_ec2_instanceid(instance_name)
    ec2 = get_ec2_resouce()
    instance = ec2.Instance(instance_id)
    ip = instance.public_ip_address
    print(f"IPアドレス: {ip}")
    return ip


def show_alive_instances():
    ec2 = get_ec2_resouce()
    instances = ec2.instances.filter(
        Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])
    i = 0
    for instance in instances:
        print(instance.id, instance.instance_type)
        i += 1


def ec2_start_from_id(instance_name, stopIs=False):
    instance_id = find_ec2_instanceid(instance_name)
    ec2 = get_ec2_resouce()
    instance = ec2.Instance(instance_id)
    if stopIs:
        print("インスタンス起動開始")
        instance.stop()
        instance.wait_until_stopped()
        print("インスタンス停止完了")
    else:
        print("インスタンス起動開始")
        instance.start()
        instance.wait_until_running()
        print("インスタンス起動完了")


# @pysnooper.snoop()
def exec_ec2(instance_name, exec_cmd):
    instance_id = find_ec2_instanceid(instance_name)
    ec2 = get_ec2_resouce()
    instance = ec2.Instance(instance_id)
    try:
        client = paramiko.SSHClient()
        client.known_hosts = None
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        privkey = paramiko.RSAKey.from_private_key_file(PEM_KEY)
        client.connect(instance.public_dns_name,username='ec2-user', pkey=privkey)
        stdin, stdout, stderr = client.exec_command(exec_cmd)
        stdin.flush()
        data = stdout.read().splitlines()
        for line in data:
            x = line.decode()
            print(line.decode())
            client.close()

    except Exception as e:
        print(e)


if __name__ == '__main__':

    instance_name = "twurl4"
    exec_cmd = "python3 hello.py"
    exec_ec2(instance_name, exec_cmd)
    pdb.set_trace()
