import os
import sys
from os import listdir
from os.path import join, isfile
from boto.s3.connection import S3Connection
import boto
from filechunkio import FileChunkIO
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



def main(args):
	s3transfer_config= {
		'multipart_threshold':8 * 1024 * 1024,
		'max_concurrency':50,
		'use_threads':True,
		'num_download_attempts':10
	}
	a={
		'ubuntu_user_name':'ubuntu',
		's3_aws_access_key_id' : 'AKIAJAUEYBYRCPNUQRKA',
		's3_aws_secret_access_key':'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',
		'git_sha':'4ed41a4c9becb1cf70d068d167498cdebc1ff37c',
		'git_user_name' :'agkgeorge',
		'git_password' : 'Panch56!',
	    'git_organization_name' : 'motioniq',
		'git_repo_name' : 'deepLearning.git',
		'ec2_region_name' :'us-west-2',
		'ami_id': 'ami-d4c0d6ad', #automation_jun27_2017_2
		'ec2_aws_access_key_id' : 'AKIAIXAEOFK2NBGJOYTA',
		'ec2_aws_secret_access_key' : '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',
		'caffemodel_relpath_cluster': 'best/cluster_june_10_266000.caffemodel',
		'caffemodel_relpath_trunk': 'best/trunk_june_10_400000.caffemodel',
		'check_detection_status_timeout_in_sec_for_status_chk' : 2*60,
		'check_detection_status_time_in_sec_interval_between_status_checks':60,
		's3transfer_config':s3transfer_config
	}

	output_subdir = 'output'
	source_dir = os.path.dirname(current_path)
	output_rootdir = os.path.join(source_dir, output_subdir)
	if not os.path.isdir(output_rootdir):
		os.makedirs(output_rootdir)


	with file(os.path.join(output_rootdir, 'default_config.yml'), 'w') as ymlstrm:
		try:
			yaml.dump(a, ymlstrm, default_flow_style=True)
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


