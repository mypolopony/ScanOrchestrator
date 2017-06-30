import os,sys
import numpy as np
from PIL import Image
import glob
import json
import boto3
import logging
import argparse
import sys
import zipfile
import glob
import shutil




def get_zip_filesnames(bucket_name=None, prefix_path=None, aws_access_key_id=None, aws_secret_access_key=None):

    assert(aws_access_key_id)
    assert(aws_secret_access_key)
    s3_res = boto3.resource('s3',
                         aws_access_key_id=aws_access_key_id,
                         aws_secret_access_key=aws_secret_access_key
    )

    bucket=s3_res.Bucket(bucket_name)
    #files_in_target_as_objects=list(bucket.objects.filter(Prefix=prefix_path))
    filenames_in_target=[os.path.split(str(o.key))[1] for o in list(bucket.objects.filter(Prefix=prefix_path))]
    return filenames_in_target


def save_txt_record(save_path, txt_record):
    with file(save_path, 'w') as f_cr:
        json.dump(txt_record, f_cr)


def get_all_files_under_common_prefix_dir(common_prefix_dir):
    """
    assuming the following folder structure,
    get the list of paths of jpg image-names sorted

    common_prefix_dir
        dir1
            lots of jpg imgs
        dir2
            lots of jpg imgs
    """
    drs_cp=[]
    if common_prefix_dir:
        for rt, drs, fls in os.walk(common_prefix_dir):
            drs_cp=[ os.path.join(rt, d) for d in drs]
            break


    test_frames=[]
    for d in drs_cp:
        d=os.path.join(common_prefix_dir, d)
        dname=os.path.split(d)[1]
        #output_dir_for_this=os.path.join(output_subdir_path, dname)
        #if not os.path.isdir(output_dir_for_this):
        #    os.makedirs(output_dir_for_this)

        test_frames_this_dir=glob.glob(d + '/*.jpg')
        test_frames_this_dir.sort()
        test_frames.extend(test_frames_this_dir)
    return test_frames



def get_head_component(path):
    """
    ("get_head_component('/')", '/.')
    ("get_head_component('foo')", 'foo')
    ("get_head_component('/foo')", '/foo')
    ("get_head_component('foo/bar')", 'foo')
    ("get_head_component('foo/bar/biff')", 'foo')
    ("get_head_component('/foo/bar')", '/foo')
    ("get_head_component('/foo/bar/biff')", '/foo')
    """
    #does not run in windows
    assert(os.name != 'nt')

    b_was_abspath=False
    if os.path.isabs(path):
        b_was_abspath=True
        path=os.path.relpath(path, '/')
    h,t=os.path.split(path)
    while h:
        h,t=os.path.split(h)
    if b_was_abspath:
        t = '/' + t
    return t



def get_rest_of_cmpnts__after(full_path, cmpnt):

    ret=[]
    h=full_path
    h,t=os.path.split(h)
    while t and t != cmpnt:
        ret.insert(0, t)
        h,t=os.path.split(h)
    return '' if (not ret) else os.path.join(*ret)

def download_zipfiles(bucket_name, zipfile_list, dataroot_dir, local_extract_root_dir=None, aws_access_key_id=None, aws_secret_access_key=None):
    logging.debug(('downloading zipfiles', bucket_name,  zipfile_list, dataroot_dir, local_extract_root_dir))
    if aws_access_key_id and aws_secret_access_key:
        s3=boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    else:
        s3 = boto3.client('s3')
    local_folders=[]
    folder_path=get_rest_of_cmpnts__after(dataroot_dir, bucket_name)
    local_common_folder_to_extract_to=dataroot_dir
    if local_extract_root_dir:
        local_common_folder_to_extract_to=os.path.join(local_extract_root_dir, bucket_name, folder_path)
    #args.folder_path=local_common_folder_to_extract_to
    for l in zipfile_list:
        local_filepath=os.path.join(dataroot_dir,  l)
        if not os.path.isdir(os.path.dirname(local_filepath)):
            os.makedirs(os.path.dirname(local_filepath))
        assert(os.path.isdir(os.path.dirname(local_filepath)))
        #logging.debug(('downloading zipfile', os.path.join(folder_path, l), local_filepath))
        if not os.path.isfile(local_filepath):
            s3.download_file(bucket_name, os.path.join(folder_path, l), local_filepath)
            

        if os.path.isfile(local_filepath):
            local_folder_to_extract_to =local_common_folder_to_extract_to
            logging.debug(('downloaded zipfile','local_filepath', local_filepath, 'local_folder_to_extract_to', local_folder_to_extract_to))
            if not os.path.isdir(local_folder_to_extract_to):
                os.makedirs(local_folder_to_extract_to)
            assert(os.path.isdir(local_folder_to_extract_to))
            zip_ref = zipfile.ZipFile(local_filepath, 'r')
            zip_ref.extractall(local_folder_to_extract_to)
            local_folders.append(os.path.splitext(os.path.split(local_filepath)[1])[0])
    return local_folders



if __name__ == "__main__":
    pass

