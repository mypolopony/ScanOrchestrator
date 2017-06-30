
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
import signal


#relative path import
#mechanism to dynamically include the relative path where utility python modules are kept to the module search path.
from inspect import getsourcefile
current_path = os.path.abspath(getsourcefile(lambda:0))
source_dir=os.path.dirname(current_path)
parent_dir = os.path.split(os.path.dirname(current_path))[0]
sys.path.insert(0, parent_dir)

class BenignException(Exception):
    def __init__(self):
        self.value = 'Unhandled / Benign Exception'
    def __str__(self):
        return repr(self.value)

class TimeoutError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class StatusWriter(object):
    def __init__(self, status_filepath=None):
        assert(status_filepath)
        self.status_filepath=status_filepath
        self.progress_pcnt=0.0
        pass

    def __enter__(self):
        assert(not os.path.isfile(self.status_filepath))
        with file(self.status_filepath, 'w') as st:
            st.write("%0.2f" % self.progress_pcnt)
        return self
    def set_progress_pcnt(self, progress_pcnt):
        self.progress_pcnt=progress_pcnt

    def __exit__(self, exception_type, exception_value, traceback):
        with file(self.status_filepath, 'w') as st:
            if exception_type is None and exception_value is None:
                st.write("100")
            elif exception_type == TimeoutError or exception_type == BenignException:
                st.write("%0.2f" % self.progress_pcnt)
                pass
            else:
                st.write("-1")
        pass


def compute_instance_name(config, args, k):
    return '%s-%s-%d' % (config['name_prefix_for_instance'], args.session_name, k)


def convert_back_slash_to_foward_slash_in_pathname(pathname):
    return pathname.replace('\\', '/')


def forward_slash_to_back_slash_in_pathname(pathname):
    return pathname.replace('/', '\\')


def upload_files(args, config, zip_files_which_need_transfer=None):
    s3_res = boto3.resource('s3',
                         aws_access_key_id=config['s3_aws_access_key_id'],
                         aws_secret_access_key=config['s3_aws_secret_access_key']
    )

    bucket=s3_res.Bucket(args.bucket_name)
    for z in zip_files_which_need_transfer:
        folder_path_in_bucket=convert_back_slash_to_foward_slash_in_pathname(
            os.path.join(args.session_name, os.path.split(args.folder_path)[1], os.path.split(z)[1])
        )
        bucket.upload_file(z, folder_path_in_bucket)
    pass


def get_zip_filesnames(bucket_name=None, config=None, prefix_path=None):

    s3_res = boto3.resource('s3',
                         aws_access_key_id=config['s3_aws_access_key_id'],#''AKIAJAUEYBYRCPNUQRKA',
                         aws_secret_access_key=config['s3_aws_secret_access_key']#'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'
    )

    bucket=s3_res.Bucket(bucket_name)
    #files_in_target_as_objects=list(bucket.objects.filter(Prefix=prefix_path))
    filenames_in_target=[os.path.split(str(o.key))[1] for o in list(bucket.objects.filter(Prefix=prefix_path))]
    return filenames_in_target



def compute_partition_result(N_resources=0, agents=None):

    N_agents=len(agents)
    assert(N_agents > 0)
    l=int(math.ceil(float(N_resources)/N_agents))
    assert(l>=0)
    partition_result={}
    partition_result={}
    for i in range(0, N_resources, l):
        partition_result[agents[i/l]]=(i,  min(i+l, N_resources))
    return partition_result



class Timeout:
    def __init__(self, seconds=1, error_message='Timeout'):
        self.seconds = seconds
        self.error_message = error_message
    def handle_timeout(self, signum, frame):
        logging.debug(('Timeout error msg:', self.error_message))
        raise TimeoutError(self.error_message)
        pass
    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)
    def __exit__(self, type, value, traceback):
        signal.alarm(0)



if __name__ == "__main__":
    pass





