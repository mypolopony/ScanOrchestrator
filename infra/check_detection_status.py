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
import re
import infra_common
import eventlet
import copy


#relative path import
#mechanism to dynamically include the relative path where utility python modules are kept to the module search path.
from inspect import getsourcefile
current_path = os.path.abspath(getsourcefile(lambda:0))
source_dir=os.path.dirname(current_path)
parent_dir = os.path.split(os.path.dirname(current_path))[0]
sys.path.insert(0, parent_dir)

def compute_instance_name(config, args, k):
    return '%s-%s-%d' % (args.name_prefix_for_instance, args.session_name, k)


def convert_back_slash_to_foward_slash_in_pathname(pathname):
    return pathname.replace('\\', '/')


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


def get_s3_filesize(client,  s3_bucket, s3_key_prefix):
    response = client.head_object(Bucket=s3_bucket, Key=s3_key_prefix)
    size_in_s3 = response['ContentLength']
    return size_in_s3

def get_zip_filesnames(s3_client, args, prefix_path=None):
    """
    s3_res = boto3.resource('s3',
                         aws_access_key_id=config['s3_aws_access_key_id'],#''AKIAJAUEYBYRCPNUQRKA',
                         aws_secret_access_key=config['s3_aws_secret_access_key']#'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'
    )
    """
    filenames_in_target=[]
    contents=s3_client.list_objects(Bucket=args.bucket_name, Prefix=prefix_path).get('Contents', None)
    if not contents:
        logging.error(('++++++++++', args.bucket_name, '----', prefix_path, '<<<<', '%r' % contents))
    else:
        filenames_in_target=[os.path.split(f['Key'])[1] for f in s3_client.list_objects(Bucket=args.bucket_name, Prefix=prefix_path)['Contents']]
        #bucket=s3_client.Bucket(args.bucket_name)
        #files_in_target_as_objects=list(bucket.objects.filter(Prefix=prefix_path))
        #filenames_in_target=[os.path.split(str(o.key))[1] for o in list(bucket.objects.filter(Prefix=prefix_path))]
    return filenames_in_target



def check_detection_output(s3_client, args, config):
    prefix_path_in=convert_back_slash_to_foward_slash_in_pathname(
        os.path.join(args.session_name, "videos")
    )
    zip_filenames_in=set(get_zip_filesnames(s3_client, args, prefix_path=prefix_path_in))


    prefix_path_out=convert_back_slash_to_foward_slash_in_pathname(
        os.path.join(args.session_name, "videos_out")
    )
    zip_filenames_out=set(get_zip_filesnames(s3_client, args, prefix_path=prefix_path_out))

    assert(len(zip_filenames_out - zip_filenames_in)==0)

    return zip_filenames_in, zip_filenames_out


def check_detection_progress(s3_client, args, config):
    prefix_path_out=convert_back_slash_to_foward_slash_in_pathname(
        os.path.join(args.session_name, "videos_out")
    )
    zip_filenames_out=set(get_zip_filesnames(s3_client, args, prefix_path=prefix_path_out))
    total_file_size=0
    for z in zip_filenames_out:
        total_file_size += int(get_s3_filesize(s3_client,  args.bucket_name, prefix_path_out + '/' + z ))
    return total_file_size

class DetectHang(object):
    def __init__(self):
        self.history=[]

    def add(self, last_reported_size, current_reported_size):
        if last_reported_size != 0 and  current_reported_size !=0:
            self.history.append((last_reported_size, current_reported_size))
        logging.debug(('DetectHang: %r', self.history))

    def check_hang(self):
        if len(self.history) < 40:
            return False
        bHang=True
        for (p1,p2) in self.history:
            bHang = bHang and  p1 != p2
        return bHang



def main(args, config):
    s3_client = boto3.client('s3',  aws_access_key_id=config['s3_aws_access_key_id'],#''AKIAJAUEYBYRCPNUQRKA',
                         aws_secret_access_key=config['s3_aws_secret_access_key'])
    i_iterations=0
    init_log(args)

    last_reported_size=0
    last_reported_z_in=0
    last_reported_z_out=0
    last_reported_f_progress=0.0
    try:
        with infra_common.StatusWriter(args.status_filepath) as st_wr:

            with eventlet.timeout.Timeout(config['check_detection_status_timeout_in_sec_for_status_chk'],
                                          infra_common.TimeoutError(
                                                      'Status check for %s timed out: ' % os.path.basename(__file__))):

                current_reported_size=check_detection_progress(s3_client, args, config )
                last_reported_size=current_reported_size
                z_in, z_out=check_detection_output(s3_client, args, config)
                last_reported_z_in=z_in
                last_reported_z_out=z_out
                num_left=len(z_in - z_out)

                f_progress = 0.0
                if len(last_reported_z_in) > 0:
                    f_progress = float(len(last_reported_z_out)) / len(last_reported_z_in)
                last_reported_f_progress = f_progress
                st_wr.set_progress_pcnt(last_reported_f_progress * 100)

gi

    except infra_common.TimeoutError,e:
        logging.error((e))
        pass




def parse_args():
    output_subdir = 'output'
    source_dir = os.path.dirname(current_path)
    output_rootdir = os.path.join(source_dir, output_subdir)
    parser=argparse.ArgumentParser('detection script')
    default_config_path=os.path.join(output_rootdir, 'default_config.yml')
    default_folder_path=r'c:\Users\Administrator\Desktop\videos'
    default_upload_bucket='sunworld_file_transfer'
    default_session_name=None
    default_expected_prefix='22005520_2017-05-13'
    parser.add_argument('-c', '--config_filepath', help='config filepath',
                        dest='config_filepath', default=default_config_path)
    parser.add_argument('-f', '--folder_path', help='common prefix to  folder containing zipped files',  dest='folder_path', default=default_folder_path)
    parser.add_argument('-n', '--session_name', help='session name', dest='session_name', required=True)
    parser.add_argument('-b', '--bucket_name', help='bucket name', dest='bucket_name', default=default_upload_bucket)
    parser.add_argument('-r', '--status_filepath', help='path to status file', dest='status_filepath', default=None,
                        required=True)
    args=parser.parse_args()
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




