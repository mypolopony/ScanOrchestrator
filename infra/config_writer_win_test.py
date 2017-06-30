import os
import sys
from os import listdir
from os.path import join, isfile
from boto.s3.connection import S3Connection
import boto
import math
import yaml
import argparse
from pprint import pprint
import logging
import config_writer_win

# relative path import
# mechanism to dynamically include the relative path where utility python modules are kept to the module search path.
from inspect import getsourcefile

current_path = os.path.abspath(getsourcefile(lambda: 0))
source_dir = os.path.dirname(current_path)
parent_dir = os.path.split(os.path.dirname(current_path))[0]
sys.path.insert(0, parent_dir)


def init_log(args):
    # start logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    # create console handler with a higher log level
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)
    logging.debug('Started')


def main(args):
    config_dict_test = {

        'tag_key': 'foo',
        'tag_value': 'orchestrator',

        're_for_win_instance_name': '(?P<prefix>\S+_)(?P<index>\d+)',

        'rvm_generate_batch_args': [0],
        'preprocess_batch_args': [1, 2, 1],
        'shape_estimation_batch_args': [2, 2],
        'process_batch_args': [3, 2, 1],
        'postprocess_batch_args': [4],
        'matlab_kill_batch_args': [-1],

        'rvm_bucket_name': 'deeplearning_data',
        'rvm_key_in_bucket': 'dummy',
        'rvm_pattern_re': '.*[.]csv',

        'num_child_matlab_instances_to_be_spawned': 2,
        'num_ubuntu_instances_to_be_spawned_from_each_win_inst': 2,
        'timeout_in_sec_for_status_chk': 60,

        'git_pull_and_config': [{'operation': 'raw_power_shell_cmd',
                                 'cmd_str': [
                                     # 'Ag_check_current_dir %s ' % config_writer_win.working_dir,
                                     'git remote rm origin',
                                     'git remote add  origin "https://' + config_writer_win.git_user_name + ':' + config_writer_win.git_password + '@github.com/' + config_writer_win.git_organization_name + '/' + config_writer_win.git_repo_name + '"',
                                     'git fetch --all ',
                                     'git merge origin/master',
                                     'git checkout master',
                                 ]
                                 },
                                {'operation': 'execute_python',
                                 'python_script': r'config_writer.py',
                                 'py_args': []
                                 }
                                ],

        'clear_folder_and_sync_code': [
            {'operation': 'removeexisting_single_folder',
             'folderpath': r'c:\Users\Administrator\Desktop\videos'},

            {'operation': 'removeexisting_single_folder',
             'folderpath': r'c:\Users\Administrator\Desktop\code'},

            {'operation': 'create_if_necessary_single_folder',
             'folderpath': r'c:\Users\Administrator\Desktop\videos'},
            {'operation': 'removeexisting_single_folder',
             'folderpath': r'c:\Users\Administrator\Desktop\results'},

            {'operation': 'create_if_necessary_single_folder',
             'folderpath': r'c:\Users\Administrator\Desktop\results'},

            {'operation': 'copy_removeexisting_unzip_single_folder',
             'bucket_name': 'prasadagridata',
             'key_in_bucket': 'latest/code.zip',
             'local_dest_dir': r'c:\Users\Administrator\Desktop'},

            {'operation': 're_pattern_remove_existing_copy',
             'pattern_re': '.*[.]csv',
             'remove_from_where': r'c:\Users\Administrator\Desktop\videos'},
            {'operation': 'replace_pattern_copy',
             'bucket_name': 'deeplearning_data',
             'pattern_replace': 'dummy/pattern',
             'local_dest_dir': r'c:\Users\Administrator\Desktop\videos'},
        ],
    }
    output_subdir = 'output'
    source_dir = os.path.dirname(current_path)
    output_rootdir = os.path.join(source_dir, output_subdir)
    if not os.path.isdir(output_rootdir):
        os.makedirs(output_rootdir)

    a = config_writer_win.config_dict
    a.update(config_dict_test)
    with file(os.path.join(output_rootdir, 'default_config_win.yml'), 'w') as ymlstrm:
        try:
            yaml.dump(a, ymlstrm, default_flow_style=True)
        except yaml.YAMLError as exc:
            logging.error(('writing config to file', exc))
    logging.info('wrote config')
    pass


def parse_args():
    parser = argparse.ArgumentParser('detection script')
    args = parser.parse_args()
    print(args)
    return(args)


if __name__ == "__main__":
    args = parse_args()
    main(args)
    pass
