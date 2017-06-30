#author kgeorge
#adapted from https://github.com/awslabs/aws-python-sample
from pprint import pprint
from boto3.s3.transfer import S3Transfer, TransferConfig
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
import shutil


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

def generate_script(args, config, k, v):
    user_name=config['ubuntu_user_name']
    git_user_name=config['git_user_name']#''agkgeorge'
    git_password=config['git_password']#'Panch56!'
    git_organization_name=config['git_organization_name']
    git_repo_name=config['git_repo_name']
    sha_to_merge=config['git_sha']#''a22d4bee891f6c52036ba8e2e981adef04495074'
    caffe_model='cluster_all_colors_may_11_2017/all_color_84000.caffemodel'
    folder_path_in_ubuntu=os.path.split(args.folder_path)[1]
    remote_dest_dir= convert_back_slash_to_foward_slash_in_pathname(
        os.path.join('caffe', 'data', 'production', args.bucket_name, args.session_name, folder_path_in_ubuntu))
    out_file= 'temp.txt'

    s=""
    key_val='#' + 'instance_name=' + compute_instance_name(config, args, k )
    working_dir='/home/' + user_name + '/code/projects/deepLearning'
    s += 'echo ' + key_val + ' >> ' + convert_back_slash_to_foward_slash_in_pathname(os.path.join(remote_dest_dir, out_file))
    s += ' \n '
    for z in v:
        s += 'echo ' + os.path.split(z)[1] + ' >> ' + convert_back_slash_to_foward_slash_in_pathname(os.path.join(remote_dest_dir, out_file))
        s += '\n '
    #s += './caffe/test/solve.sh -i '+ os.path.join(working_dir, remote_dest_dir, out_file) + '  &'
    #s += ' \n '
    s_cd = ' \n '
    s_cd += 'cd ' + working_dir
    s_cd += ' \n '
    s_cd += 'pwd >> /tmp/data.txt'
    s_cd += ' \n '
    s_cd += 'mkdir -p ' + remote_dest_dir
    s_cd += ' \n '
    s_cd += 'chown -R ' + ' %r:%r '%(user_name,user_name)  + ' caffe/data'
    s_cd += ' \n '
    #s_cd += 'git fetch ' + '"https://' + git_user_name + ':' + git_password + '@github.com/' + git_organization_name +'/' + git_repo_name+ '"' + ' master'
    #s_cd += ' \n '
    #s_cd += 'git merge ' + sha_to_merge
    #s_cd += ' \n '
    s_cd_post=' \n '
    s_cd_post += ' python caffe/test/solve.py -i ' +  remote_dest_dir + '/' + out_file
    s_cd_post +=  ' -s ' + args.session_name + ' -t '  + 'grape_640x512' + ' -b  2 '
    s_cd_post +=  ' -c ' +config['caffemodel_relpath_cluster'] + ' -u ' + config['caffemodel_relpath_trunk'] + ' -e '
    s_cd_post +=  ' -k ' + config['s3_aws_access_key_id'] + ' -y ' + config['s3_aws_secret_access_key']
    s_cd_post += ' \n '

    user_data_script = """#!/bin/bash
    """ + s_cd + s + s_cd_post
    return user_data_script


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

def upload_files(s3transfer, args, config, zip_files_which_need_transfer=None):

    for z in zip_files_which_need_transfer:
        folder_path_in_bucket=convert_back_slash_to_foward_slash_in_pathname(
            os.path.join(args.session_name, os.path.split(args.folder_path)[1], os.path.split(z)[1])
        )
        s3transfer.upload_file(z, args.bucket_name, folder_path_in_bucket)
    pass


def partition_zip_files(zip_files_which_need_transfer=None, num_instances=0):
    s=set(zip_files_which_need_transfer)


    assert(num_instances > 0)
    l=int(math.ceil(float(len(zip_files_which_need_transfer))/args.num_instances))
    if l <= 0:
        logging.debug(('found 0 zip files exiting'))
        sys.exit(0)

    partition_result={}
    for i in range(0, len(zip_files_which_need_transfer), l):
        min(i+l, len(zip_files_which_need_transfer))
        zip_files_for_this_instance=zip_files_which_need_transfer[i:min(i+l, len(zip_files_which_need_transfer))]
        print(i, len(zip_files_for_this_instance), zip_files_for_this_instance)
        for z in zip_files_for_this_instance:
            s.remove(z)
        partition_result[i/l]=zip_files_for_this_instance
        assert(len(zip_files_for_this_instance)>0)
    return partition_result

def get_zip_filesnames(bucket_name=None, prefix_path=None):

    s3_res = boto3.resource('s3',
                         aws_access_key_id=config['s3_aws_access_key_id'],#''AKIAJAUEYBYRCPNUQRKA',
                         aws_secret_access_key=config['s3_aws_secret_access_key']#'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'
    )

    bucket=s3_res.Bucket(args.bucket_name)
    #files_in_target_as_objects=list(bucket.objects.filter(Prefix=prefix_path))
    filenames_in_target=[os.path.split(str(o.key))[1] for o in list(bucket.objects.filter(Prefix=prefix_path))]
    return filenames_in_target



def verify_that_zip_files_are_in_s3(args, config, partitions):
    prefix_path=convert_back_slash_to_foward_slash_in_pathname(
        os.path.join(args.session_name, os.path.split(args.folder_path)[1])
    )
    zip_filenames=get_zip_filesnames(bucket_name=args.bucket_name, prefix_path=prefix_path)
    missing_zip_files=[]
    for k, v in partitions.iteritems():
        for z in v:
            zip_filename=os.path.split(z)[1]
            if zip_filename not in zip_filenames:
                missing_zip_files.append(zip_filename)

    pprint(('+' * 30, 'missing'))
    if missing_zip_files:
        pprint(missing_zip_files)
        pprint(zip_filenames)
        raise RuntimeError('missing zip files')
    pprint(('+' * 30, 'end missing'))

def start_instances(args, config,  partitions=None):
    ec2 = boto3.resource('ec2', region_name=config['ec2_region_name'],
                                      aws_access_key_id=config['ec2_aws_access_key_id'],
                                      aws_secret_access_key=config['ec2_aws_secret_access_key'])
    for k,v  in partitions.iteritems():
        res=ec2.create_instances(ImageId=config['ami_id'], InstanceType=args.instance_type, KeyName='agp2_2', MinCount=1, MaxCount=1,
                                 SecurityGroupIds=['sg-547fef2c'],  UserData=generate_script(args, config,  k, v))
                #SecurityGroupIds=['sg-547fef2c'], UserData=generate_script(args, k, v))
        instance = res[0]
        response = instance.create_tags(
            Tags=[
                {
                    'Key': 'foo',
                    'Value': 'bar'
                },
            ]
        )

        response = instance.create_tags(
            Tags=[
                {
                    'Key': 'Name',
                    'Value': compute_instance_name(config, args, k)
                },
            ])

        while instance.state == u'pending':
            print "Instance state: %s" % instance.state
            time.sleep(10)
            instance.update()
        logging.debug(('*********************launched instance,', k))

    filters = [{
        'Name': 'tag:foo',
        'Values': ['bar']
        }]
    all_instances_created = ec2.instances.filter(Filters=filters)
    pprint(all_instances_created)

def zip_up_files(args):
    expected_prefix_re=re.compile(args.expected_prefix, re.I)
    zip_filenames=[]
    zip_temp_dir=os.path.join(args.folder_path, args.session_name)
    if os.path.isdir(zip_temp_dir):
        logging.warning(('removing previously existing temporary folder, ',  zip_temp_dir))
        shutil.rmtree(zip_temp_dir)
    assert(not os.path.isdir(zip_temp_dir))
    if not os.path.isdir(zip_temp_dir):
        os.makedirs(zip_temp_dir)
    for rt, drs, fls in os.walk(args.folder_path):
        for d in drs:
            #if d.startswith(args.expected_prefix):
            if expected_prefix_re.match(d):
                logging.debug(('~~~~~~~~~~~~~~~~~~~~~~ ', d , '============='))
                zip_filenames.append(os.path.join(zip_temp_dir,  d +  '.zip'))
                with zipfile.ZipFile(os.path.join(zip_temp_dir, d + '.zip'), 'w', allowZip64=True) as z:
                    top_level_jpg_files = glob.glob(os.path.join(rt, d) + '/*.jpg')
                    top_level_mat_files = glob.glob(os.path.join(rt, d) + '/*.mat')
                    for f in top_level_mat_files:
                        archivename = os.path.join(d, os.path.split(f)[1])
                        z.write(f, archivename)
                    for f in top_level_jpg_files:
                        archivename = os.path.join(d, os.path.split(f)[1])
                        z.write(f, archivename)
                pass
        break
    #assert(not os.path.isdir(os.path.join(args.folder_path, args.session_name)))
    #os.makedirs(os.path.join(args.folder_path, args.session_name))
    #for z in zip_filenames:
    #    logging.debug(('zip_filename', z))
    return zip_filenames




def main(args, config):
    s3client = boto3.client('s3',
                      aws_access_key_id=config['s3_aws_access_key_id'],
                      aws_secret_access_key=config['s3_aws_secret_access_key'])
    s3config = TransferConfig(
        multipart_threshold=config['s3transfer_config']['multipart_threshold'],
        max_concurrency=config['s3transfer_config']['max_concurrency'],
        num_download_attempts=config['s3transfer_config']['num_download_attempts'],
        use_threads=config['s3transfer_config']['use_threads'],
    )
    s3transfer = S3Transfer(s3client, s3config)
    total_time_taken_file_xfer=0
    with infra_common.StatusWriter(args.status_filepath) as st_wr:
        try:
            init_log(args)
            zip_files_which_need_transfer=zip_up_files(args)
            pprint(len(zip_files_which_need_transfer))
            pprint('================================')
            partitions=partition_zip_files(zip_files_which_need_transfer=zip_files_which_need_transfer, num_instances=args.num_instances)
            start_upload=time.time()
            pprint('================================')
            upload_files(s3transfer, args, config,  zip_files_which_need_transfer)
            total_time_taken_file_xfer +=time.time() - start_upload
            verify_that_zip_files_are_in_s3(args, config,  partitions)
            start_instances(args, config, partitions=partitions)
        except Exception as ex:
            raise
        logging.info(('spawn_instances.file_transfer_time_taken_in_sec:', total_time_taken_file_xfer))




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
    parser.add_argument('-k', '--num_instances', help='number of instances that need be spawned', default=3, dest='num_instances')
    parser.add_argument('-n', '--session_name', help='session name', dest='session_name', required=True)
    parser.add_argument('-t', '--instance_type', help='type of the instance one of [p2.xlarge, p2.8xlarge, p2.16xlarge] ', default='p2.xlarge',  dest='instance_type')
    parser.add_argument('-p', '--expected_prefix', help='expected prefix', default=default_expected_prefix, dest='expected_prefix')
    parser.add_argument('-b', '--bucket_name', help='bucket name', dest='bucket_name', default=default_upload_bucket)
    parser.add_argument('-x', '--name_prefix_for_instance', help='name_prefix_for_instance', dest='name_prefix_for_instance', required=True)
    parser.add_argument('-r', '--status_filepath', help='path to status file', dest='status_filepath', default=None,
                        required=True)
    args=parser.parse_args()
    args.num_instances = int(args.num_instances)
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




