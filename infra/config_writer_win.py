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




#relative path import
#mechanism to dynamically include the relative path where utility python modules are kept to the module search path.
from inspect import getsourcefile
current_path = os.path.abspath(getsourcefile(lambda:0))
source_dir=os.path.dirname(current_path)
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

git_user_name='agkgeorge'
git_password='Panch56!'
git_organization_name='motioniq'
git_repo_name='infra.git'

working_dir=r'C:\Users\Administrator\Documents\projects\infra'
data_dir=r'C:\Users\Administrator\Desktop\videos'

config_dict={
		#The following s3-old id/key pair is registered for koshy.
		#Please use your appropriate id/key pair here
		's3_aws_access_key_id' : 'AKIAJAUEYBYRCPNUQRKA',
		's3_aws_secret_access_key':'rMoz/LA4Ml/Sztv+eIPU3qxfWD19kAeDwszyAfQO',

		'instance_type': 'c4.8xlarge',

		'ec2_region_name' :'us-east-1',
		#THe following ec2 id/key pair is registered for the user ssm_user_1
		'ec2_aws_access_key_id': 'AKIAIGIYEZZ5V3WUAD6Q',
		'ec2_aws_secret_access_key': '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',
		'tag_key': 'foo',
		'tag_value':'bar_kgeorge_3',
		'working_dir': working_dir,
		'data_dir': data_dir,
		're_for_win_instance_name': '(?P<prefix>\S+_)(?P<index>\d+)',

		'ssm_send_command_TimeoutSeconds' : 360,
		'ssm_send_command_OutputS3Region' : 'us-east-1',
		'ssm_send_command_OutputS3BucketName' : 'foo_delme',
	  	'ssm_send_command_OutputS3KeyPrefix' :  'send_command_result',

		'rvm_generate_batch_args': [0],
		'preprocess_batch_args': [1, 25, 0],
		'shape_estimation_batch_args': [2, 25],
		'process_batch_args': [3, 25, 0],
		'postprocess_batch_args': [4],
		'matlab_kill_batch_args': [-1],

		'rvm_bucket_name': 'prasadagridata',
		'rvm_key_in_bucket': 'latest',
		'rvm_pattern_re': '.*[.]csv',
		'timeout_in_sec_for_status_chk': 60,

		'num_child_matlab_instances_to_be_spawned': 25,
		'num_ubuntu_instances_to_be_spawned_from_each_win_inst':3,


	    'git_pull_and_config': [{'operation': 'raw_power_shell_cmd',
							 'cmd_str': [
                                     # 'Ag_check_current_dir %s ' % config_writer_win.working_dir,
                                     'git remote rm origin',
                                     'git remote add  origin "https://' + git_user_name + ':' + git_password + '@github.com/' + git_organization_name + '/' + git_repo_name + '"',
                                     'git fetch --all ',
                                     'git config --global user.email "bot@agridata.ai"',
                                     'git config --global user.name "Windows Instance Bot"',
                                     'git merge origin/master',
                                     'git checkout --theirs .',
                                     'git add .',
                                     'git commit -m "merge"',
                                     'git checkout master',
                                 ]
							 },
							{'operation': 'execute_python',
							 'python_script': r'config_writer.py',
							 'py_args': []
							 }
							],

	#
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

			{	'operation' : 're_pattern_remove_existing_copy',
					 'pattern_re': '.*[.]csv',
					 'remove_from_where': r'c:\Users\Administrator\Desktop\videos'},

			{	'operation' :'replace_pattern_copy',
			 'bucket_name': 'prasadagridata',
			 'pattern_replace': 'latest/pattern',
			 'local_dest_dir': r'c:\Users\Administrator\Desktop\videos'},

		],
		'code_xfer': [
			{'operation': 'removeexisting_single_folder',
			 'folderpath': r'c:\Users\Administrator\Desktop\code'},
			{'operation': 'copy_removeexisting_unzip_single_folder',
			 'bucket_name': 'prasadagridata',
			 'key_in_bucket': 'latest/code.zip',
			 'local_dest_dir': r'c:\Users\Administrator\Desktop'},
		],
		'prepare_for_process': [
			{'operation': 'removeexisting_single_folder',
			 'folderpath': r'c:\Users\Administrator\Desktop\results'},

			{'operation': 'create_if_necessary_single_folder',
			 'folderpath': r'c:\Users\Administrator\Desktop\results'},
		],

		'per_instance_operations_post': [
			{'operation': 'raw_power_shell_cmd',
			 'cmd_str': ['Set-Location -Path %s' % (r'C:\Users\Administrator\Desktop\videos'), 'matlab.exe -nosplash -nodesktop -logfile logfile.txt -r "batch(0, 2, 1)"']
			 },
		],

	}


def main(args):

	output_subdir = 'output'
	source_dir = os.path.dirname(current_path)
	output_rootdir = os.path.join(source_dir, output_subdir)
	if not os.path.isdir(output_rootdir):
		os.makedirs(output_rootdir)


	with file(os.path.join(output_rootdir, 'default_config_win.yml'), 'w') as ymlstrm:
		try:
			yaml.dump(config_dict, ymlstrm, default_flow_style=True)
		except yaml.YAMLError as exc:
			logging.error(('writing config to file', exc))
	logging.info('wrote config')
	pass




def parse_args():
    parser=argparse.ArgumentParser('detection script')
    args=parser.parse_args()
    pprint((args))
    return args



if __name__ == "__main__":
    args=parse_args()
    main(args)
    pass


