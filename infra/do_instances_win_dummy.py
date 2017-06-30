#author kgeorge
#adapted from https://github.com/awslabs/aws-python-sample
from pprint import pprint
import boto3
import uuid
import time
import math
import os
import argparse
import logging
import sys
import glob
import yaml
import zipfile
import json
import datetime
import csv
import infra_common
import re
import eventlet
import copy

#relative path import
#mechanism to dynamically include the relative path where utility python modules are kept to the module search path.
from inspect import getsourcefile
current_path = os.path.abspath(getsourcefile(lambda:0))
source_dir=os.path.dirname(current_path)
parent_dir = os.path.split(os.path.dirname(current_path))[0]
sys.path.insert(0, parent_dir)
#import gen_utils

def init_log(args):
    #start logger
    logger=logging.getLogger()
    logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    # create console handler with a higher log level
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)
    logging.debug('Started')








def construct_execute_python_bkgd_cmd_old(args, config, operand_dict):
    space = " "
    py_args = operand_dict['py_args'][:]
    py_args.append( '--status_filepath')
    py_args.append(operand_dict['status_filepath'])
    s = "Start-Process -NoNewWindow "
    s += "python.exe "
    s += space
    s += " -ArgumentList  \""
    s += space
    s += operand_dict['python_script']
    s += space
    s += " ".join([ str(a) for a in py_args])
    s += "\""
    s += space
    logging.debug(('++++++++;;;;;;;PYTHION BKGD;;;;;;;++++++++++++++', s))

    retval = ['Set-Location -Path %s' % (config['working_dir']), '. ag_tools.ps1', ' Ag_force_remove_file ' + convert_back_slash_to_foward_slash_in_pathname(operand_dict['status_filepath'])]
    retval.append(s)
    return retval



def wait_step_command_fnisihes(ssm, ssmCommand):
    status = 'Pending'
    while status == 'Pending' or status == 'InProgress':
        time.sleep(3)
        cmd_struct = (ssm.list_commands(CommandId=ssmCommand['Command']['CommandId']))['Commands']
        status = (ssm.list_commands(CommandId=ssmCommand['Command']['CommandId']))['Commands'][0][
            'Status']
    if (status != 'Success'):
        logging.debug(('send_command failed'))
        raise RuntimeError


def run_matlab(args, config, relevant_instances, ssm, name_of_operation='', check_status_only=False):
    instance_cmd_registry={}
    for inst in relevant_instances:
        try:
            ssmCommand = ssm.send_command(InstanceIds=[inst["InstanceId"]], DocumentName='AWS-RunPowerShellScript',
                                          Comment='time_consuming_operation %s' % name_of_operation,
                                          TimeoutSeconds=config['ssm_send_command_TimeoutSeconds'],
                                          #ServiceRoleArn='arn:aws:iam::561396111812:role/SSM_role',
                                          OutputS3Region=config['ssm_send_command_OutputS3Region'],
                                          OutputS3BucketName=config['ssm_send_command_OutputS3BucketName'],
                                          OutputS3KeyPrefix=config['ssm_send_command_OutputS3KeyPrefix'],
                                          Parameters={"commands": ['notepad.exe ']})
            wait_step_command_fnisihes(ssm, ssmCommand)
            instance_cmd_registry.update({inst["InstanceId"]:ssmCommand})
        except Exception:
            raise


    return


def do_job(args, config):

    """
    try:
        with infra_common.Timeout(seconds=3, error_message='myerr'):
            time.sleep(4)
    except infra_common.TimeoutError:
        pass
    """

    ssm = boto3.client('ssm', region_name=config['ec2_region_name'],
                         aws_access_key_id=config['ec2_aws_access_key_id'],
                         aws_secret_access_key=config['ec2_aws_secret_access_key'])
    pprint((dir(ssm)))

    client = boto3.client('ec2',region_name=config['ec2_region_name'],
                         aws_access_key_id=config['ec2_aws_access_key_id'],
                         aws_secret_access_key=config['ec2_aws_secret_access_key'])

    filters = [{
        'Name': 'tag:%s' % config['tag_key'],
        'Values': [config['tag_value']]
    },
    {'Name': 'instance-state-name',
     #'Values': ['stopped', 'terminated']}]
     'Values': ['running']}]

    class DatetimeEncoder(json.JSONEncoder):
      def default(self, obj):
        #if isinstance(obj, datetime):
        #    return obj.strftime('%Y-%m-%dT%H:%M:%SZ')
        if isinstance(obj, datetime.date):
            return obj.strftime('%Y-%m-%d')
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)



    output = json.dumps((client.describe_instances(Filters=filters)), cls=DatetimeEncoder)
    output_dict=json.loads(output)
    reservations=output_dict.get('Reservations', None)
    #pprint(dir(ssm))


    relevant_instances=[]
    re_for_win_instance_name=re.compile(config['re_for_win_instance_name'], re.I)
    for g in reservations:
        r1=g.get('Instances', None)
        if not r1:
            continue
        for r in r1:
            assert(r['Tags'])
            m=re_for_win_instance_name.match(filter(lambda t: t['Key'] == 'Name', r['Tags'])[0]['Value'])
            assert(m)
            logging.debug(('===================', ' running cmd prefix: %s, suffix:(%d, %s)' % (m.group('prefix'), int(m.group('index')), r['InstanceId'])))
            relevant_instances.append({'InstanceId': r.get('InstanceId', None), 'Tag_Index':int(m.group('index'))})
            break

    relevant_instances=sorted(relevant_instances, key=lambda d: d['Tag_Index'])
    instance_ids=[v['InstanceId'] for v in relevant_instances]

    # copy files
    rinput = 'n'
    if not args.b_force:
        print('The following instances will be affected by this', instance_ids)
        rinput = raw_input('Proceed y/n?:')
    if rinput == 'y':

        run_matlab(args, config, relevant_instances, ssm,
                   name_of_operation='', check_status_only=False)
    pass






def main(args, config):
    s3client = boto3.client('s3')
    try:
        init_log(args)
        do_job(args, config)
    except Exception as ex:
        raise





def parse_args():
    output_subdir = 'output'
    source_dir = os.path.dirname(current_path)
    output_rootdir = os.path.join(source_dir, output_subdir)
    parser=argparse.ArgumentParser('detection script')
    default_config_path=os.path.join(output_rootdir, 'default_config_win.yml')
    default_folder_path=r'c:\Users\Administrator\Desktop\videos_kg_delme'
    default_scan_folder='59055036037c2fc5e372ad9d'
    default_upload_bucket='sunworld_file_transfer'
    default_session_name=None
    default_expected_prefix='22005520_2017-05-13'
    parser.add_argument('-c', '--config_filepath', help='config filepath',
                        dest='config_filepath', default=default_config_path)
    parser.add_argument('-f', '--folder_path', help='common prefix to  folder containing zipped files',  dest='folder_path', default=default_folder_path)
    parser.add_argument('-k', '--num_instances', help='number of instances that need be spawned', default=3, dest='num_instances')
    parser.add_argument('-n', '--session_name', help='session name', dest='session_name', required=True)
    parser.add_argument('-p', '--expected_prefix', help='expected prefix', default=default_expected_prefix, dest='expected_prefix')
    parser.add_argument('-b', '--bucket_name', help='bucket name', dest='bucket_name', default=default_upload_bucket)
    parser.add_argument('-s', '--scan_folder', help='scan_folder', dest='scan_folder', default=default_scan_folder)
    parser.add_argument('-o', '--check_only', help='check_status_only', action='store_true',
                        dest='check_status_only')
    parser.add_argument('--force', help='force', action='store_true',
                        dest='b_force')
    parser.add_argument('-g', '--stage', help='stage', type=int, dest='stage', default=0)

    args=parser.parse_args()
    args.num_instances = int(args.num_instances)
    args.output_dir = os.path.join(output_rootdir, args.session_name)
    if not os.path.isdir(args.output_dir ):
        os.makedirs(args.output_dir)
    assert(args.num_instances > 1)
    pprint((args))
    return args

if __name__ == "__main__":
    args=parse_args()

    config_filepath=args.config_filepath
    with file(config_filepath) as fp:
        config=yaml.load(fp)

    pprint(('config', config))
    main(args, config)
    pass
