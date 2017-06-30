# author kgeorge
# adapted from https://github.com/awslabs/aws-python-sample
import argparse
import copy
import csv
import datetime
import json
import logging
import math
import os
import re
import sys
import time
from inspect import getsourcefile
from pprint import pprint

import boto3
import eventlet
import yaml

import infra_common

# Configuration
current_path = os.path.abspath(getsourcefile(lambda: 0))
source_dir = os.path.dirname(current_path)
parent_dir = os.path.split(os.path.dirname(current_path))[0]
sys.path.insert(0, parent_dir)
output_subdir = 'output'
output_rootdir = os.path.join(source_dir, output_subdir)
config_path = os.path.join(output_rootdir, 'default_config_win.yml')
with file(config_path) as fp:
    config = yaml.load(fp)

# S3 Connection
s3 = boto3.client('s3', aws_access_key_id=config['s3_aws_access_key_id'],
                  aws_secret_access_key=config['s3_aws_secret_access_key'])


class ProcessError(RuntimeError):
    def __init___(self, cmd_id, instance_id):
        bucket = 'agridatadepot'
        prefix = '/'.join(
            ['foo_delme', 'send_command_result', cmd_id, instance_id, 'awsrunPowerShellScript/stderr.txt'])
        log = s3.Object(bucket, prefix)
        logging.ERROR(log)
        raise RuntimeError


def commandinfo(inst, cmd_list, cmd_id):
    logging.info('Instance: {}. Running commands ({}):'.format(inst, cmd_id))
    for idx, cmd in enumerate(cmd_list):
        logging.info('\t{}) {}'.format(idx + 1, cmd))


def compute_instance_name(config, args, k):
    return '%s-%s-%d' % (config['name_prefix_for_instance'], args.session_name, k)


def convert_back_slash_to_foward_slash_in_pathname(pathname):
    return pathname.replace('\\', '/')


def convert_forward_slash_to_back_slash_in_pathname(pathname):
    return pathname.replace('/', '\\')


class PathnameConverter(object):
    def __init__(self, is_win_target=None):
        if not is_win_target:
            self.is_win_target = False
            if sys.platform == 'win32':
                self.is_win_target = True
        else:
            self.is_win_target = is_win_target

    def cnvrt(self, pathname):
        if self.is_win_target:
            return self.convert_forward_slash_to_back_slash_in_pathname(pathname)
        else:
            return self.convert_back_slash_to_foward_slash_in_pathname(pathname)

    @staticmethod
    def convert_forward_slash_to_back_slash_in_pathname(pathname):
        return pathname.replace('/', '\\')

    @staticmethod
    def convert_back_slash_to_foward_slash_in_pathname(pathname):
        return pathname.replace('\\', '/')


def init_log(args):
    # start logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # create file handler which logs even debug messages
    # create console handler with a higher log level
    # ch = logging.StreamHandler(sys.stdout)
    # ch.setLevel(logging.DEBUG)
    # logger.addHandler(ch)
    # logging.debug('Started')


def partition_zip_files(zip_files_which_need_transfer=None, num_instances=0):
    s = set(zip_files_which_need_transfer)

    assert (num_instances > 0)
    l = int(math.ceil(float(len(zip_files_which_need_transfer)) / args.num_instances))
    if l <= 0:
        logging.debug(('found 0 zip files exiting'))
        sys.exit(0)

    partition_result = {}
    for i in range(0, len(zip_files_which_need_transfer), l):
        min(i + l, len(zip_files_which_need_transfer))
        zip_files_for_this_instance = zip_files_which_need_transfer[i:min(i + l, len(zip_files_which_need_transfer))]
        print(i, len(zip_files_for_this_instance), zip_files_for_this_instance)
        for z in zip_files_for_this_instance:
            s.remove(z)
        partition_result[i / l] = zip_files_for_this_instance
        assert (len(zip_files_for_this_instance) > 0)
    return partition_result


def get_core_rvm(args, config):
    rvm_bucket_name = config['rvm_bucket_name']
    rvm_key_in_bucket = config['rvm_key_in_bucket']
    rvm_pattern_re = config['rvm_pattern_re']
    rvm_pattern_re_engine = re.compile(rvm_pattern_re, re.I)
    objects_in_prefix_dir = infra_common.get_zip_filesnames(bucket_name=rvm_bucket_name, config=config,
                                                            prefix_path=rvm_key_in_bucket)
    core_rvm_filename = filter(lambda o_name: rvm_pattern_re_engine.match(o_name) is not None, objects_in_prefix_dir)
    logging.debug(('++++++++++++++++++++++, rvm entry', core_rvm_filename))
    assert (len(core_rvm_filename) == 1)
    return core_rvm_filename[0]


def construct_from_args_s3_copy_cmd(args, config, operand_dict, cmd_list):
    bucket_name = operand_dict['bucket_name']
    key_in_bucket = operand_dict['key_in_bucket']
    local_dest_dir = operand_dict['local_dest_dir']
    s3_aws_access_key_id = config.get('s3_aws_access_key_id')
    s3_aws_secret_access_key = config.get('s3_aws_secret_access_key')
    local_dest_path = convert_forward_slash_to_back_slash_in_pathname(
        os.path.join(local_dest_dir, os.path.split(key_in_bucket)[1]))

    s = 'Copy-S3Object  '
    s += '  -BucketName ' + bucket_name
    s += ' -Key  ' + key_in_bucket
    s += '  -LocalFile ' + local_dest_path
    s += ' -AccessKey ' + s3_aws_access_key_id
    s += ' -SecretKey ' + s3_aws_secret_access_key
    cmd_list.append(s)
    return cmd_list


def construct_s3_copy_and_unzip_cmd_old(args, config, operand_dict, cmd_list):
    bucket_name = operand_dict['bucket_name']
    key_in_bucket = operand_dict['key_in_bucket']
    local_dest_dir = operand_dict['local_dest_dir']
    s3_aws_access_key_id = config.get('s3_aws_access_key_id')
    s3_aws_secret_access_key = config.get('s3_aws_secret_access_key')
    local_dest_path = convert_forward_slash_to_back_slash_in_pathname(
        os.path.join(local_dest_dir, os.path.split(key_in_bucket)[1]))

    if os.path.splitext(local_dest_path)[1] != '.zip':
        raise ValueError('zip file is expected bug got %s' % local_dest_path)

    local_folder = operand_dict.get('local_folder', None)
    if not local_folder:
        local_folder = os.path.splitext(local_dest_path)[0]

    s = 'Copy-S3Object  '
    s += '  -BucketName ' + bucket_name
    s += ' -Key  ' + key_in_bucket
    s += '  -LocalFile ' + local_dest_path
    s += ' -AccessKey ' + s3_aws_access_key_id
    s += ' -SecretKey ' + s3_aws_secret_access_key
    cmd_list.append(s)
    '''
    s = 'Expand-Archive '
    s += local_dest_path
    s += ' -DestinationPath '
    s += local_folder

    retval.append(s)
    '''

    return cmd_list


def construct_s3_copy_and_unzip_cmd(args, config, operand_dict, cmd_list):
    bucket_name = operand_dict['bucket_name']
    key_in_bucket = operand_dict['key_in_bucket']
    local_dest_dir = operand_dict['local_dest_dir']
    s3_aws_access_key_id = config.get('s3_aws_access_key_id')
    s3_aws_secret_access_key = config.get('s3_aws_secret_access_key')
    local_dest_path = convert_forward_slash_to_back_slash_in_pathname(
        os.path.join(local_dest_dir, os.path.split(key_in_bucket)[1]))

    if os.path.splitext(local_dest_path)[1] != '.zip':
        raise ValueError('zip file is expected bug got %s' % local_dest_path)

    local_folder = operand_dict.get('local_folder', None)
    if not local_folder:
        local_folder = os.path.splitext(local_dest_path)[0]
    cmd_list.extend(['Set-Location -Path %s' % (config['working_dir']), '. %s' % (r'powershell\ag_tools.ps1')])
    s = 'Ag_copy_removeexisting_unzip_single_folder ' + bucket_name + '  ' + key_in_bucket + ' ' + local_dest_path + ' ' + s3_aws_access_key_id + ' ' + s3_aws_secret_access_key
    cmd_list.append(s)
    return cmd_list


def construct_removeexisting_single_folder_cmd(args, config, operand_dict, cmd_list):
    folderpath_raw = operand_dict['folderpath']
    folderpath = convert_forward_slash_to_back_slash_in_pathname(folderpath_raw)
    cmd_list.extend(['Set-Location -Path %s' % (config['working_dir']), '. %s' % (r'powershell\ag_tools.ps1')])
    s = 'Ag_removeexisting_single_folder ' + folderpath
    cmd_list.append(s)
    return cmd_list


def construct_create_if_necessary_single_folder_cmd(args, config, operand_dict, cmd_list):
    folderpath_raw = operand_dict['folderpath']
    folderpath = convert_forward_slash_to_back_slash_in_pathname(folderpath_raw)
    cmd_list.extend(['Set-Location -Path %s' % (config['working_dir']), '. %s' % (r'powershell\ag_tools.ps1')])
    s = 'Ag_create_if_necessary_single_folder ' + folderpath
    cmd_list.append(s)
    return cmd_list


def construct_re_pattern_remove_existing_copy(args, config, operand_dict, cmd_list):
    pattern_re = operand_dict['pattern_re']
    remove_from_where = operand_dict['remove_from_where']
    cmd_list.extend(
        ['Set-Location -Path %s' % (config['working_dir']), '$env:Path', '. %s' % (r'powershell\ag_tools.ps1')])
    s = 'Ag_re_pattern_remove_existing_copy ' + pattern_re + '  ' + remove_from_where
    cmd_list.append(s)
    return cmd_list


def construct_execute_python_cmd(args, config, operand_dict, cmd_list):
    s = 'python.exe '
    s += operand_dict['python_script']
    s += ' '
    s += ' '.join([str(a) for a in operand_dict['py_args']])
    s += ' '
    logging.debug(('++++++++;;;;;;;;;;;;;;++++++++++++++', s))
    cmd_list.extend(['Set-Location -Path %s' % (config['working_dir']), '. %s' % (r'powershell\ag_tools.ps1')])
    cmd_list.append(s)
    return cmd_list


def construct_raw_power_shell_cmd(args, config, operand_dict):
    cmds = ['Set-Location -Path %s' % (config['working_dir'])]
    cmds.extend(operand_dict['cmd_str'])
    return cmds


def construct_execute_python_bkgd_cmd(args, config, operand_dict):
    space = " "
    args = operand_dict['py_args'][:]
    args.append('--status_filepath')
    args.append(operand_dict['status_filepath'])
    s = "Start-Process  "
    s += "python.exe "
    s += space
    s += " -ArgumentList  ("
    # s += "c:/Users/Administrator/Documents/projects/infra/dummy_bkgd_job.py -t 7 --status_filepath c:/Users/Administrator/Desktop/videos/dummy_s3.txt"
    s += "dummy_bkgd_job.py -t 7 --status_filepath dummy_s3.txt"
    # s +=  "c:/Users/Administrator/Documents/projects/infra/dwnld_from_s3.py -m c:/Users/Administrator/Desktop/videos -s wiinst_990_delme --status_filepath c:/Users/Administrator/Desktop/videos/dwnld_from_s3.txt"
    # s += space
    # s += operand_dict['python_script']
    # s += space
    # s += " ".join([ str(a) for a in args])
    s += ")"
    s += " -NoNewWindow "
    s += space
    logging.debug(('++++++++;;;;;;;PYTHION BKGD;;;;;;;++++++++++++++', s))

    retval = ['Set-Location -Path %s' % (config['working_dir']), '. %s' % (r'powershell\ag_tools.ps1'),
              ' Ag_force_remove_file ' + convert_back_slash_to_foward_slash_in_pathname(
                  operand_dict['status_filepath'])]
    retval.append(s)
    return retval


def construct_execute_python_bkgd_cmd_old(args, config, operand_dict):
    space = " "
    py_args = operand_dict['py_args'][:]
    py_args.append('--status_filepath')
    py_args.append(operand_dict['status_filepath'])
    s = "Start-Process -NoNewWindow "
    s += "python.exe "
    s += space
    s += " -ArgumentList  \""
    s += space
    s += operand_dict['python_script']
    s += space
    s += " ".join([str(a) for a in py_args])
    s += "\""
    s += space
    logging.debug(('++++++++;;;;;;;PYTHION BKGD;;;;;;;++++++++++++++', s))

    retval = ['Set-Location -Path %s' % (config['working_dir']), '. %s' % (r'powershell\ag_tools.ps1'),
              ' Ag_force_remove_file ' + convert_back_slash_to_foward_slash_in_pathname(
                  operand_dict['status_filepath'])]
    retval.append(s)
    return retval


def parse_operand(args, config, operand_dict, cmd_list):
    if operand_dict['operation'] == 'from_args_copy_removeexisting':
        return cmd_list
    elif operand_dict['operation'] == 'replace_pattern_copy':
        return construct_replace_pattern_copy_cmd(args, config, operand_dict, cmd_list)
    elif operand_dict['operation'] == 'copy_removeexisting_unzip_single_folder':
        return construct_s3_copy_and_unzip_cmd(args, config, operand_dict, cmd_list)

    elif operand_dict['operation'] == 'removeexisting_single_folder':
        return construct_removeexisting_single_folder_cmd(args, config, operand_dict, cmd_list)

    elif operand_dict['operation'] == 'create_if_necessary_single_folder':
        return construct_create_if_necessary_single_folder_cmd(args, config, operand_dict, cmd_list)
    elif operand_dict['operation'] == 're_pattern_remove_existing_copy':
        return construct_re_pattern_remove_existing_copy(args, config, operand_dict, cmd_list)
    elif operand_dict['operation'] == 'execute_python':
        return construct_execute_python_cmd(args, config, operand_dict, cmd_list)
    elif operand_dict['operation'] == 'raw_power_shell_cmd':
        cmd_list.append('Set-Location -Path %s' % (config['working_dir']))
        cmd_list.append('. %s' % (r'powershell\ag_tools.ps1'))
        cmd_list.extend(operand_dict['cmd_str'])
        return cmd_list
    else:
        raise NotImplementedError


def construct_replace_pattern_copy_cmd(args, config, operand_dict, cmd_list):
    if operand_dict['operation'] == 'replace_pattern_copy':
        core_rvm = get_core_rvm(args, config)
        operand_dict['key_in_bucket'] = operand_dict['pattern_replace'].replace('pattern', core_rvm)
        return construct_from_args_s3_copy_cmd(args, config, operand_dict, cmd_list)


def get_zip_filesnames(bucket_name=None, prefix_path=None):
    s3_res = boto3.resource('s3',
                            aws_access_key_id=config['s3_aws_access_key_id'],  # ''AKIAJAUEYBYRCPNUQRKA',
                            aws_secret_access_key=config['s3_aws_secret_access_key']
                            # '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'
                            )

    bucket = s3_res.Bucket(args.bucket_name)
    # files_in_target_as_objects=list(bucket.objects.filter(Prefix=prefix_path))
    filenames_in_target = [os.path.split(str(o.key))[1] for o in list(bucket.objects.filter(Prefix=prefix_path))]
    return filenames_in_target


def construct_synchronoize_wait_raw_power_shell_cmd(args, config, operand_dict):
    pass


def construct_s3_copy_cmd_old(args, config, bucket_name=None, key_in_bucket=None, local_dest_dir=None):
    s3_code_bucket_name = config.get('s3_code_bucket_name')
    s3_code_path = config.get('s3_code_path')
    s3_code_dest_dir = config.get('s3_code_dest_dir')
    s3_aws_access_key_id = config.get('s3_aws_access_key_id')
    s3_aws_secret_access_key = config.get('s3_aws_secret_access_key')
    s3_code_dest_path = convert_forward_slash_to_back_slash_in_pathname(
        os.path.join(s3_code_dest_dir, os.path.split(s3_code_path)[1]))
    s = 'Copy-S3Object  '
    s += '  -BucketName ' + s3_code_bucket_name
    s += ' -Key  ' + s3_code_path
    s += '  -LocalFile ' + s3_code_dest_path
    s += ' -AccessKey ' + s3_aws_access_key_id
    s += ' -SecretKey ' + s3_aws_secret_access_key
    return s


def attach_status_filepath(operand_dict_with_status_filepath_and_args):
    py_args = operand_dict_with_status_filepath_and_args['py_args']
    py_args.append('--status_filepath')
    py_args.append(operand_dict_with_status_filepath_and_args['status_filepath'])
    assert ('--status_filepath' in operand_dict_with_status_filepath_and_args['py_args'])


def wait_step_command_finishes(ssm, ssmCommand):
    status = 'Pending'
    while status == 'Pending' or status == 'InProgress':
        cmd_struct = (ssm.list_commands(CommandId=ssmCommand['Command']['CommandId']))['Commands']
        status = (ssm.list_commands(CommandId=ssmCommand['Command']['CommandId']))['Commands'][0][
            'Status']

        logging.info('\t{}\t({})'.format(status, ssmCommand['Command']['CommandId']))
        time.sleep(2)
    if (status != 'Success'):
        logging.debug(('send_command failed'))
        raise ProcessError(cmd_struct, ssmCommand)


def synchronous_operation(
        args, config,
        relevant_instances, operand_dict_process,
        fn_update_per_instance_args,
        ssm, name_of_operation=''):
    instance_cmd_registry = {}
    for inst in relevant_instances:
        try:
            operand_dict_process_for_this_inst = copy.deepcopy(operand_dict_process)
            fn_update_per_instance_args(inst, operand_dict_process_for_this_inst)
            cmd_list = ['Set-Location -Path %s' % (config['working_dir']), '. %s' % (r'powershell\ag_tools.ps1')]
            cmd_list = parse_operand(args, config, operand_dict_process_for_this_inst, cmd_list)
            logging.debug(('===========(((((((((()))))))))===============', cmd_list))
            ssmCommand = ssm.send_command(InstanceIds=[inst["InstanceId"]], DocumentName='AWS-RunPowerShellScript',
                                          Comment='time_consuming_operation %s' % name_of_operation,
                                          TimeoutSeconds=config['ssm_send_command_TimeoutSeconds'],
                                          # ServiceRoleArn='arn:aws:iam::561396111812:role/SSM_role',
                                          OutputS3Region=config['ssm_send_command_OutputS3Region'],
                                          OutputS3BucketName=config['ssm_send_command_OutputS3BucketName'],
                                          OutputS3KeyPrefix=config['ssm_send_command_OutputS3KeyPrefix'],
                                          Parameters={"commands": cmd_list})
            commandinfo(inst, cmd_list, ssmCommand['Command']['CommandId'])
            wait_step_command_finishes(ssm, ssmCommand)

            cmd_invocation = ssm.get_command_invocation(CommandId=ssmCommand['Command']['CommandId'],
                                                        InstanceId=inst["InstanceId"])

            instance_cmd_registry.update({inst["InstanceId"]: cmd_invocation})

        except Exception as e:
            logging.error(str(e))

    return instance_cmd_registry


def time_consuming_operation(args, config, relevant_instances, operand_dict_process, fn_update_per_instance_args, ssm,
                             name_of_operation='', check_status_only=False):
    instance_cmd_registry = {}
    if not check_status_only:
        for inst in relevant_instances:
            try:
                operand_dict_process_for_this_inst = copy.deepcopy(operand_dict_process)
                fn_update_per_instance_args(inst, operand_dict_process_for_this_inst)
                attach_status_filepath(operand_dict_process_for_this_inst)
                cmd_list = ['Set-Location -Path %s' % (config['working_dir']), '. %s' % (r'powershell\ag_tools.ps1'),
                            'Ag_force_remove_file %s' % (operand_dict_process_for_this_inst['status_filepath'])]
                cmd_list = parse_operand(args, config, operand_dict_process_for_this_inst, cmd_list)
                logging.debug(('===========(((((((((()))))))))===============', cmd_list))
                ssmCommand = ssm.send_command(InstanceIds=[inst["InstanceId"]], DocumentName='AWS-RunPowerShellScript',
                                              Comment='time_consuming_operation %s' % name_of_operation,
                                              TimeoutSeconds=config['ssm_send_command_TimeoutSeconds'],
                                              # ServiceRoleArn='arn:aws:iam::561396111812:role/SSM_role',
                                              OutputS3Region=config['ssm_send_command_OutputS3Region'],
                                              OutputS3BucketName=config['ssm_send_command_OutputS3BucketName'],
                                              OutputS3KeyPrefix=config['ssm_send_command_OutputS3KeyPrefix'],
                                              Parameters={"commands": cmd_list})
                commandinfo(inst, cmd_list, ssmCommand['Command']['CommandId'])
                instance_cmd_registry.update({inst["InstanceId"]: ssmCommand})

            except Exception as e:
                logging.error(str(e))

        while True:
            b_all_success = True            # Innocent until proven guilty
            for ssmCommand in instance_cmd_registry.values():
                status = ssm.list_commands(CommandId=ssmCommand['Command']['CommandId'])['Commands'][0]['Status']
                logging.info('\t{}\t({})'.format(status, ssmCommand['Command']['CommandId']))

                if status != 'Success':
                    b_all_success = False

            if b_all_success:
                break

            eventlet.sleep(10)

    operand_dict = {
        'operation': 'raw_power_shell_cmd',
        'cmd_str': [
            '. %s' % (r'powershell\ag_tools.ps1'),
            'Ag_check_if_file_exists -filepath ' + operand_dict_process['status_filepath']
        ]
    }
    r = re.compile('Exists\s*(?P<progress>[-+]?\d*[.]?\d*)')
    results = []
    try:
        with eventlet.timeout.Timeout(config['timeout_in_sec_for_status_chk'],
                                      infra_common.TimeoutError('Status check for %s timed out: ' % name_of_operation)):
            while len(relevant_instances) > 0:
                instances_that_still_has_not_finished = []
                instances_that_reported_success_now = []
                instances_that_reported_failure_now = []
                for i in range(len(relevant_instances)):
                    inst = relevant_instances[i]
                    try:
                        cmd_list = construct_raw_power_shell_cmd(args, config, operand_dict)
                        logging.debug(('===========(((((((((()))))))))===============', cmd_list))
                        ssmCommand = ssm.send_command(InstanceIds=[inst["InstanceId"]],
                                                      DocumentName='AWS-RunPowerShellScript',
                                                      Comment='check status for %s' % name_of_operation,
                                                      TimeoutSeconds=60,
                                                      OutputS3Region=config['ssm_send_command_OutputS3Region'],
                                                      OutputS3BucketName=config['ssm_send_command_OutputS3BucketName'],
                                                      OutputS3KeyPrefix=convert_back_slash_to_foward_slash_in_pathname(
                                                          os.path.join(config['ssm_send_command_OutputS3KeyPrefix'],
                                                                       'status_check')
                                                      ),
                                                      Parameters={"commands": cmd_list})
                        commandinfo(inst, cmd_list, ssmCommand['Command']['CommandId'])
                        wait_step_command_finishes(ssm, ssmCommand)

                        cmd_invocation = ssm.get_command_invocation(CommandId=ssmCommand['Command']['CommandId'],
                                                                    InstanceId=inst["InstanceId"])
                        m = r.match(cmd_invocation['StandardOutputContent'])

                        if m:
                            logging.debug(('&&&&&&&&&&&&&&&&&&&&&&&&&&&&&  Progress %s, ==== %s' % (
                            cmd_invocation['StandardOutputContent'], m.group('progress'))))
                            if m.group('progress') is not 'NaN':
                                relevant_instances[i]['progress'] = float(m.group('progress'))
                        if relevant_instances[i].get('progress', 0.0) >= 100.0:
                            relevant_instances[i]['result'] = 'Success'
                        elif relevant_instances[i].get('progress', 0.0) < 0.0:
                            relevant_instances[i]['result'] = 'Failure'
                        else:
                            relevant_instances[i]['result'] = 'InProgress'

                    except Exception:
                        raise
                eventlet.sleep(1)
    except infra_common.TimeoutError, e:
        logging.debug(('not complete success!'))
    finally:
        # per instance status report
        logging.debug(('still missing status text %s from instances:%r' % (
        operand_dict_process['status_filepath'], [v['Tag_Index'] for v in relevant_instances])))
        logging.debug(('instances that reported success from instances:%r' % (
        [v['Tag_Index'] for v in results if v['result'] == 'success'])))
        logging.debug(('instances that reported failure from instances:%r' % (
            [v['Tag_Index'] for v in results if v['result'] == 'failure'])))

    return relevant_instances


def construct_s3_remove_local_cp_cmd(args, config, bucket_name=None, key_in_bucket=None, local_dest_dir=None):
    assert (bucket_name)
    assert (key_in_bucket)
    assert (local_dest_dir)
    s3_aws_access_key_id = config.get('s3_aws_access_key_id')
    s3_aws_secret_access_key = config.get('s3_aws_secret_access_key')
    local_dest_path = convert_forward_slash_to_back_slash_in_pathname(
        os.path.join(local_dest_dir, os.path.split(key_in_bucket)[1]))

    s = 'Remove-Item  '
    s += local_dest_path
    return s


def get_video_names(args, config):
    core_rvm_name = get_core_rvm(args, config)
    pthnm_cnvrter = PathnameConverter(is_win_target=False)
    logging.debug(('**********************', core_rvm_name))
    with open(os.path.join(args.output_dir, core_rvm_name), 'wb') as data:
        try:
            s3_prefix = config['rvm_key_in_bucket'] + '/' + core_rvm_name
            s3.download_fileobj(config['rvm_bucket_name'], s3_prefix, data)
        except Exception as e:
            print('There was a problem!')
            print(str(e))

    video_names = set([])
    # with open('/Users/mypolopony/Downloads/row_video_map_may_15_16_ivory.csv', 'rbU') as f:
    with open(os.path.join(args.output_dir, core_rvm_name), 'rbU') as f:
        reader = csv.reader(f)
        count = 0
        for row in reader:
            if not count:
                count += 1
                continue
            else:
                video_names.add(row[4])
                count += 1
    logging.debug(('$$$$$$$$$$$$$$$$$$$$$$$$$ %r' % video_names))
    return list(video_names)


def update_per_instance_args_detect_file_xfer_curry(args):
    def update_per_instance_args_detect_file_xfer(inst, operand_dict_process):
        operand_dict_process['py_args'].append('-s')
        operand_dict_process['py_args'].append(args.instances[inst["Tag_Index"]])

    return update_per_instance_args_detect_file_xfer


def update_per_instance_args_copy_video_files_curry(partition={}):
    def update_per_instance_args_copy_video_files(inst, operand_dict_process):
        operand_dict_process['py_args'].append('-v')
        operand_dict_process['py_args'].extend(list(partition[inst["InstanceId"]]))

    return update_per_instance_args_copy_video_files


def compute_per_instance_session_name(args, inst):
    return args.session_name + '_' + inst['Tag_Name']


def update_per_instance_args_detect_curry(args):
    def update_per_instance_args_detect(inst, operand_dict_process):
        operand_dict_process['py_args'].append('-n')
        operand_dict_process['py_args'].append(args.instances[inst["Tag_Index"]])

    return update_per_instance_args_detect


def update_per_instance_args_noop(inst, operand_dict_process):
    pass


def trigger_matlab_stage(args, relevant_instances, ssm, ):
    filepath_trigger_matlab = convert_forward_slash_to_back_slash_in_pathname(
        os.path.join(config['data_dir'], 'batch_args.txt'))
    content_string = ','.join([str(s) for s in config['%s_batch_args' % (args.stage)]])
    # content_string = '0,2,0'
    logging.debug(('%s, batch_args.txt, %s' % (args.stage, content_string)))
    operand_dict_trigger_matlab = {'operation': 'raw_power_shell_cmd',
                                              'status_filepath': convert_forward_slash_to_back_slash_in_pathname(
                                                  os.path.join(config['data_dir'], 'done.txt')),
                                              'cmd_str': [
                                                  'Ag_create_if_necessary_single_folder %s' % (config['data_dir']),
                                                  'Set-Location -Path %s' % (config['data_dir']),
                                                  'Ag_force_remove_file %s ' % convert_forward_slash_to_back_slash_in_pathname(
                                                      os.path.join(config['data_dir'], 'batch_args.txt')),
                                                  'Ag_writes_string_to_file -filepath %s  -content_string %s ' % (
                                                      filepath_trigger_matlab, content_string)
                                                  ],
                                              'py_args': []
                                              }
    relevant_instances_matlab = copy.deepcopy(relevant_instances)
    return time_consuming_operation(args, config, relevant_instances_matlab, operand_dict_trigger_matlab,
                                    update_per_instance_args_noop, ssm,
                                    name_of_operation='preprocess', check_status_only=args.check_status_only)


def verify_sorted_relevant_instances_tag_id(relevant_instances, range_list):
    s_tag_indices = set([v['Tag_Index'] for v in relevant_instances])
    s_range_list = set(range_list)
    return len(s_tag_indices - s_range_list) == 0 and len(s_range_list - s_tag_indices) == 0


def do_job(args):
    """
    try:
        with infra_common.Timeout(seconds=3, error_message='myerr'):
            time.sleep(4)
    except infra_common.TimeoutError:
        pass
    """
    results = None

    ssm = boto3.client('ssm', region_name=config['ec2_region_name'],
                       aws_access_key_id=config['ec2_aws_access_key_id'],
                       aws_secret_access_key=config['ec2_aws_secret_access_key'])
    #  logging.debug(dir(ssm))

    client = boto3.client('ec2', region_name=config['ec2_region_name'],
                          aws_access_key_id=config['ec2_aws_access_key_id'],
                          aws_secret_access_key=config['ec2_aws_secret_access_key'])

    filters = [{
        'Name': 'tag:%s' % config['tag_key'],
        'Values': [config['tag_value']]
    },
        {'Name': 'instance-state-name',
         # 'Values': ['stopped', 'terminated']}]
         'Values': ['running']}]

    class DatetimeEncoder(json.JSONEncoder):
        def default(self, obj):
            # if isinstance(obj, datetime):
            #    return obj.strftime('%Y-%m-%dT%H:%M:%SZ')
            if isinstance(obj, datetime.date):
                return obj.strftime('%Y-%m-%d')
            # Let the base class default method raise the TypeError
            return json.JSONEncoder.default(self, obj)

    output = json.dumps((client.describe_instances(Filters=filters)), cls=DatetimeEncoder)
    output_dict = json.loads(output)
    reservations = output_dict.get('Reservations', None)

    relevant_instances = list()
    re_for_win_instance_name = re.compile(config['re_for_win_instance_name'], re.I)
    for g in reservations:
        r1 = g.get('Instances', None)
        if not r1:
            continue
        for r in r1:
            assert (r['Tags'])
            tag_name = filter(lambda t: t['Key'] == 'Name', r['Tags'])[0]['Value']
            m = re_for_win_instance_name.match(tag_name)
            # filter(lambda t: t['Key'] == 'Name', r['Tags'])[0]['Value'])
            assert (m)
            logging.debug(('===================', ' running cmd prefix: %s, suffix:(%d, %s)' % (
            m.group('prefix'), int(m.group('index')), r['InstanceId'])))
            relevant_instances.append(
                {'InstanceId': r.get('InstanceId', None), 'Tag_Index': int(m.group('index')), 'Tag_Name': tag_name})

    relevant_instances = sorted(relevant_instances, key=lambda d: d['Tag_Index'])
    instance_ids = [v['InstanceId'] for v in relevant_instances]

    # copy files
    rinput = 'n'
    if not args.b_force:
        print('The following instances will be affected by this', instance_ids)
        rinput = raw_input('Proceed y/n?:')
    if args.b_force or rinput == 'y':
        if (args.stage == 'git_pull_and_config'):

            for v in config.get('git_pull_and_config', None):
                cmd_list = []
                commands = parse_operand(args, config, v, cmd_list)
                logging.debug(('+++++++++++++++++++ %r' % commands))
                # s3_cmd = construct_s3_copy_cmd(args, config,  bucket_name= bucket_name, key_in_bucket=key_in_bucket, local_dest_dir=local_dest_dir)
                ssmCommand = ssm.send_command(InstanceIds=instance_ids, DocumentName='AWS-RunPowerShellScript',
                                              Comment='la la la',
                                              TimeoutSeconds=240,
                                              OutputS3Region='us-east-1',
                                              OutputS3BucketName='foo_delme',
                                              OutputS3KeyPrefix='send_command_result',
                                              Parameters={"commands": commands})
                commandinfo(instance_ids, commands, ssmCommand['Command']['CommandId'])
                wait_step_command_finishes(ssm, ssmCommand)

        if (args.stage == 'code_xfer'):

            for v in config.get('code_xfer', None):
                cmd_list = []
                commands = parse_operand(args, config, v, cmd_list)
                logging.debug(('+++++++++++++++++++ %r' % commands))
                # s3_cmd = construct_s3_copy_cmd(args, config,  bucket_name= bucket_name, key_in_bucket=key_in_bucket, local_dest_dir=local_dest_dir)
                ssmCommand = ssm.send_command(InstanceIds=instance_ids, DocumentName='AWS-RunPowerShellScript',
                                              Comment='la la la',
                                              TimeoutSeconds=240,
                                              OutputS3Region='us-east-1',
                                              OutputS3BucketName='foo_delme',
                                              OutputS3KeyPrefix='send_command_result',
                                              Parameters={"commands": commands})
                commandinfo(instance_ids, commands, ssmCommand['Command']['CommandId'])
                wait_step_command_finishes(ssm, ssmCommand)

        if (args.stage == 'clear_folder_and_sync_code'):

            for v in config.get('clear_folder_and_sync_code', None):
                cmd_list = []
                commands = parse_operand(args, config, v, cmd_list)
                logging.debug(('+++++++++++++++++++ %r' % commands))
                # s3_cmd = construct_s3_copy_cmd(args, config,  bucket_name= bucket_name, key_in_bucket=key_in_bucket, local_dest_dir=local_dest_dir)
                ssmCommand = ssm.send_command(InstanceIds=instance_ids, DocumentName='AWS-RunPowerShellScript',
                                              Comment='la la la',
                                              TimeoutSeconds=240,
                                              OutputS3Region='us-east-1',
                                              OutputS3BucketName='foo_delme',
                                              OutputS3KeyPrefix='send_command_result',
                                              Parameters={"commands": commands})
                commandinfo(instance_ids, commands, ssmCommand['Command']['CommandId'])

                wait_step_command_finishes(ssm, ssmCommand)

        if (args.stage == 'video_xfer'):
            status_filepath = convert_forward_slash_to_back_slash_in_pathname(
                os.path.join(config['data_dir'], 'status_%s.txt' % args.stage))
            video_names = get_video_names(args, config)
            partition_results = infra_common.compute_partition_result(N_resources=len(video_names), agents=instance_ids)
            pprint(('partition_results', partition_results))
            assert (len(instance_ids) == len(partition_results.keys()))
            assert (max([v[1] for k, v in partition_results.iteritems()]) == len(video_names))

            #
            operand_dict_video_file_xfer = {'operation': 'execute_python',
                                            'python_script': r'fetch_tar_files.py',
                                            'py_args': ['-r', get_core_rvm(args, config),
                                                        '-p', args.scan_folder
                                                        ],
                                            'status_filepath': status_filepath
                                            }
            # fetch video files

            fn_update_per_instance_args_copy_video_files = update_per_instance_args_copy_video_files_curry(
                partition_results)
            relevant_instances_copy = copy.deepcopy(relevant_instances)
            return time_consuming_operation(args, config, relevant_instances_copy, operand_dict_video_file_xfer,
                                            fn_update_per_instance_args_copy_video_files, ssm,
                                            name_of_operation=args.stage, check_status_only=args.check_status_only)

        if (args.stage == 'video_xfer_check'):
            status_filepath = convert_forward_slash_to_back_slash_in_pathname(
                os.path.join(config['data_dir'], 'status_%s.txt' % args.stage))
            video_names = get_video_names(args, config)
            partition_results = infra_common.compute_partition_result(N_resources=len(video_names), agents=instance_ids)
            pprint(('partition_results', partition_results))
            assert (len(instance_ids) == len(partition_results.keys()))
            assert (max([v[1] for k, v in partition_results.iteritems()]) == len(video_names))

            #
            operand_dict_video_file_xfer = {'operation': 'execute_python',
                                            'python_script': r'fetch_tar_files.py',
                                            'py_args': ['-r', get_core_rvm(args, config),
                                                        '-p', args.scan_folder,
                                                        '-o'
                                                        ],
                                            'status_filepath': status_filepath
                                            }
            # fetch video files

            fn_update_per_instance_args_copy_video_files = update_per_instance_args_copy_video_files_curry(
                partition_results)
            relevant_instances_copy = copy.deepcopy(relevant_instances)
            return time_consuming_operation(args, config, relevant_instances_copy, operand_dict_video_file_xfer,
                                            fn_update_per_instance_args_copy_video_files, ssm,
                                            name_of_operation=args.stage, check_status_only=args.check_status_only)

        if (args.stage == 'prepare_for_process'):

            for v in config.get('prepare_for_process', None):
                cmd_list = []
                commands = parse_operand(args, config, v, cmd_list)
                logging.debug(('+++++++++++++++++++ %r' % commands))
                # s3_cmd = construct_s3_copy_cmd(args, config,  bucket_name= bucket_name, key_in_bucket=key_in_bucket, local_dest_dir=local_dest_dir)
                ssmCommand = ssm.send_command(InstanceIds=instance_ids, DocumentName='AWS-RunPowerShellScript',
                                              Comment='la la la',
                                              TimeoutSeconds=240,
                                              OutputS3Region='us-east-1',
                                              OutputS3BucketName='foo_delme',
                                              OutputS3KeyPrefix='send_command_result',
                                              Parameters={"commands": commands})
                commandinfo(instance_ids, commands, ssmCommand['Command']['CommandId'])

                wait_step_command_finishes(ssm, ssmCommand)

        if args.stage == 'restart':
            status_filepath = convert_forward_slash_to_back_slash_in_pathname(
                os.path.join(config['data_dir'], 'status_%s.txt' % args.stage))
            operand_dict_matlab = {'operation': 'raw_power_shell_cmd',

                                   'status_filepath': status_filepath,
                                   'cmd_str': [
                                       'Ag_restart_computer',
                                       # 'Ag_Write_To_File ' + matlab_status_filepath + '  100'
                                   ],
                                   'py_args': []
                                   }
            relevant_instances_copy = copy.deepcopy(relevant_instances)
            cmd_results = synchronous_operation(
                args, config,
                relevant_instances_copy, operand_dict_matlab,
                update_per_instance_args_noop, ssm, name_of_operation=args.stage
            )

        if args.stage == 'post_detect_xfer':

            status_filepath = convert_forward_slash_to_back_slash_in_pathname(
                os.path.join(config['data_dir'], 'status_%s.txt' % args.stage))
            operand_dict_detect_file_xfer = {'operation': 'execute_python',
                                             'python_script': r'dwnld_from_s3.py',
                                             'py_args': [],
                                             'status_filepath': status_filepath
                                             }
            # fetch video files


            fn_update_per_instance_args_detect_file_xfer = update_per_instance_args_detect_file_xfer_curry(
                args)

            relevant_instances_copy = copy.deepcopy(relevant_instances)

            for inst in relevant_instances_copy:
                session_name = compute_per_instance_session_name(args, inst)
                logging.debug(('per instance session name %s' % session_name))

            return time_consuming_operation(args, config, relevant_instances_copy, operand_dict_detect_file_xfer,
                                            fn_update_per_instance_args_detect_file_xfer, ssm,
                                            name_of_operation=args.stage, check_status_only=args.check_status_only)

        if args.stage == 'detection':

            status_filepath = convert_forward_slash_to_back_slash_in_pathname(
                os.path.join(config['data_dir'], 'status_%s.txt' % args.stage))

            operand_dict_detect = {'operation': 'execute_python',
                                   'python_script': r'spawn_instances.py',
                                   'py_args': ['-k',
                                               str(config['num_ubuntu_instances_to_be_spawned_from_each_win_inst']),
                                               '-p', '.*preprocess.*', '-x', 'kg_delme_blk3_trial'],
                                   'status_filepath': status_filepath
                                   }
            # fetch video files

            relevant_instances_copy = copy.deepcopy(relevant_instances)
            for inst in relevant_instances_copy:
                session_name = compute_per_instance_session_name(args, inst)
                logging.debug(('per instance session name %s' % session_name))

            fn_update_per_instance_args_detect = update_per_instance_args_detect_curry(
                args)
            return time_consuming_operation(args, config, relevant_instances_copy, operand_dict_detect,
                                            fn_update_per_instance_args_detect, ssm,
                                            name_of_operation=args.stage, check_status_only=args.check_status_only)

        if args.stage == 'check_detection_status':

            status_filepath = convert_forward_slash_to_back_slash_in_pathname(
                os.path.join(config['data_dir'], 'status_%s.txt' % args.stage))
            operand_dict_detect = {'operation': 'execute_python',
                                   'python_script': r'check_detection_status.py',
                                   'py_args': [],
                                   'status_filepath': status_filepath
                                   }

            relevant_instances_copy = copy.deepcopy(relevant_instances)
            for inst in relevant_instances_copy:
                session_name = compute_per_instance_session_name(args, inst)
                logging.debug(('per instance session name %s' % session_name))

            fn_update_per_instance_args_detect = update_per_instance_args_detect_curry(
                args)
            return time_consuming_operation(args, config, relevant_instances_copy, operand_dict_detect,
                                            fn_update_per_instance_args_detect, ssm,
                                            name_of_operation=args.stage, check_status_only=args.check_status_only)

        if args.stage == 'matlab_status':

            status_filepath = convert_forward_slash_to_back_slash_in_pathname(
                os.path.join(config['data_dir'], 'status_%s.txt' % args.stage))
            operand_dict_matlab = {'operation': 'raw_power_shell_cmd',

                                   'status_filepath': status_filepath,
                                   'cmd_str': [
                                       'Ag_check_how_many_processes MATLAB',
                                       # 'Ag_Write_To_File ' + matlab_status_filepath + '  100'
                                   ],
                                   'py_args': []
                                   }
            relevant_instances_copy = copy.deepcopy(relevant_instances)
            cmd_results = synchronous_operation(
                args, config,
                relevant_instances_copy, operand_dict_matlab,
                update_per_instance_args_noop, ssm, name_of_operation=args.stage
            )
            for k, v in cmd_results.iteritems():
                logging.debug((k, v['StandardOutputContent']))

        if args.stage in ['matlab_kill', 'preprocess', 'rvm_generate', 'postprocess', 'process']:
            return trigger_matlab_stage(args, relevant_instances, ssm)

    return results


def run(args):
    args.folder_path = r'c:\Users\Administrator\Desktop\videos_kg_delme'
    args.num_instances = len(args.instances)
    args.output_dir = os.path.join(output_rootdir, args.session_name)
    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)

    init_log(args)

    try:
        return do_job(args)
    except Exception as ex:
        raise


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
    parser = argparse.ArgumentParser('detection script')
    default_config_path = os.path.join(output_rootdir, 'default_config_win.yml')
    default_folder_path = r'c:\Users\Administrator\Desktop\videos_kg_delme'
    default_scan_folder = '59055036037c2fc5e372ad9d'
    default_upload_bucket = 'sunworld_file_transfer'
    default_session_name = None
    default_expected_prefix = '22005520_2017-05-13'
    parser.add_argument('-c', '--config_filepath', help='config filepath',
                        dest='config_filepath', default=default_config_path)
    parser.add_argument('-f', '--folder_path', help='common prefix to  folder containing zipped files',
                        dest='folder_path', default=default_folder_path)
    parser.add_argument('-k', '--num_instances', help='number of instances that need be spawned', default=3,
                        dest='num_instances')
    parser.add_argument('-n', '--session_name', help='session name', dest='session_name', required=True)
    parser.add_argument('-p', '--expected_prefix', help='expected prefix', default=default_expected_prefix,
                        dest='expected_prefix')
    parser.add_argument('-b', '--bucket_name', help='bucket name', dest='bucket_name', default=default_upload_bucket)
    parser.add_argument('-s', '--scan_folder', help='scan_folder', dest='scan_folder', default=default_scan_folder)
    parser.add_argument('-o', '--check_only', help='check_status_only', action='store_true',
                        dest='check_status_only')
    parser.add_argument('--force', help='force', action='store_true',
                        dest='b_force')
    parser.add_argument('-g', '--stage', help='stage', dest='stage', required=True)

    args = parser.parse_args()
    args.num_instances = int(args.num_instances)
    args.output_dir = os.path.join(output_rootdir, args.session_name)
    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)
    assert (args.num_instances > 1)
    pprint((args))
    return args


if __name__ == "__main__":
    args = parse_args()

    config_filepath = args.config_filepath
    with file(config_filepath) as fp:
        config = yaml.load(fp)

    pprint(('config', config))
    main(args, config)
    pass
