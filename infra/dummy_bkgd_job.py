#author kgeorge
#adapted from https://github.com/awslabs/aws-python-sample
from pprint import pprint
import time
import math
import os
import argparse
import logging
import sys
import glob
import yaml
import zipfile
import shutil
import socket
import infra_common

#relative path import
#mechanism to dynamically include the relative path where utility python modules are kept to the module search path.
from inspect import getsourcefile
current_path = os.path.abspath(getsourcefile(lambda:0))
source_dir=os.path.dirname(current_path)
parent_dir = os.path.split(os.path.dirname(current_path))[0]
sys.path.insert(0, parent_dir)

def compute_instance_name(config, args, k):
    return '%s-%s-%d' % (config['name_prefix_for_instance'], args.session_name, k)


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



def verify_every_file_in_src_in_dest(src_folder, dst_folder):
    fls_in_src=[]
    for rt, drs, fls in os.walk(src_folder):
        fls_in_src = fls[:]
        break

    fls_in_dst=[]
    for rt, drs, fls in os.walk(dst_folder):
        fls_in_dst = fls[:]
        break
    s_fls_in_src=set(fls_in_src)
    s_fls_in_dst=set(fls_in_dst)
    return len(list(s_fls_in_src - s_fls_in_dst)) == 0



def get_zip_files_to_extract(args):
    zip_files_to_extract = []
    for s in args.session_names:
        logging.debug((s))
        zip_files_to_extract_this_session = glob.glob(os.path.join(args.mother_folder_path, s, 'videos_out', '*.zip'))
        zip_files_to_extract.extend(zip_files_to_extract_this_session)
    return zip_files_to_extract


def dwnld_zip_files_from_s3(args, config=None, s3=None, dryrun=False):
    for s in args.session_names:
        folder_path=infra_common.convert_back_slash_to_foward_slash_in_pathname(os.path.join(s, 'videos_out'))
        z_names=infra_common.get_zip_filesnames(bucket_name=args.bucket_name, config=config, prefix_path=folder_path)
        for z_ in z_names:
            local_file_path=infra_common.forward_slash_to_back_slash_in_pathname(os.path.join(args.mother_folder_path, folder_path, z_))
            if not dryrun:
                if not os.path.isdir(os.path.dirname(local_file_path)):
                    os.makedirs(os.path.dirname(local_file_path))
                s3.download_file(args.bucket_name, infra_common.convert_back_slash_to_foward_slash_in_pathname(os.path.join(folder_path,z_)), local_file_path)
                assert(os.path.isfile(local_file_path))

            yield local_file_path




def verify_content(args, zip_files_to_extract):
    folders_in_mother_folder = []
    for rt, drs, fls in os.walk(args.mother_folder_path):
        folders_in_mother_folder = drs[:]
        break
    s_folders_in_mother_folder = set(folders_in_mother_folder)
    s_zip_filenames=set([os.path.splitext(os.path.split(z)[1])[0] for z in zip_files_to_extract])
    if len(list(s_zip_filenames - s_folders_in_mother_folder)) > 0:
        raise ValueError ('%r, these zip folders not present in mother folder' % (list(s_zip_filenames - s_folders_in_mother_folder)))
    pprint(('missing zip file folders', list(s_folders_in_mother_folder - s_zip_filenames)))

    return


def main(args, config):
    try:
        time.sleep(args.sleep_time)
        raise ValueError('hihi')
    except ValueError as e:
        pass
        #raise infra_common.TimeoutError('my bad')
    finally :
        with file(args.status_filepath, 'w') as st:
            st.write("1")
    pass





def parse_args():
    output_subdir = 'output'
    source_dir = os.path.dirname(current_path)
    output_rootdir = os.path.join(source_dir, output_subdir)
    parser=argparse.ArgumentParser('detection script')
    default_config_path=os.path.join(output_rootdir, 'default_config.yml')
    default_mother_folder_path=r'c:\Users\Administrator\Desktop\videos'
    default_bucket='sunworld_file_transfer'
    default_session_name=None
    default_expected_prefix='22005520_2017-05-13'
    parser.add_argument('-c', '--config_filepath', help='config filepath',
                        dest='config_filepath', default=None)
    parser.add_argument('-g', '--config_str', help='config content as string',
                        dest='config_str', default=None)

    parser.add_argument('-r', '--status_filepath', help='path to status file', dest='status_filepath', default=None, required=True)
    parser.add_argument('-m', '--mother_folder_path', help='path to mother folder',  dest='mother_folder_path', default=default_mother_folder_path)
    #parser.add_argument('-f', '--working folder of zip files', help='number of instances that need be spawned',  dest='working_folder_path')
    parser.add_argument('-s', '--session_names', help='one or more session names',
                        dest='session_names', nargs='+')
    parser.add_argument('-b', '--bucket_name', help='bucket name', dest='bucket_name', default=default_bucket)
    parser.add_argument('-t', '--sleep_time', help='sleep time', dest='sleep_time', type=float)

    args=parser.parse_args()
    pprint((args))
    return args


if __name__ == "__main__":
    args=parse_args()

    config = {}
    if args.config_filepath:
        config_filepath=args.config_filepath
        with file(config_filepath) as fp:
            config=yaml.load(fp)
    elif args.config_str:
        config=yaml.load(args.config_str)

    pprint(('config', config))
    main(args, config)
    pass




