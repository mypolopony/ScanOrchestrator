import os,sys
import boto3
import time
import csv
import re
import argparse
import shutil
import tarfile
from pprint import pprint
import logging
import infra_common
import glob
from PIL import Image



def xofy(x,y):
    print('{} / {} bytes'.format(x,y))




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



client = boto3.client(
    's3',
    aws_access_key_id='AKIAJCTBYUIK37F3OUYA',
    aws_secret_access_key='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',
)


def halve_img(fullpath):
    frame_img = Image.open(fullpath)
    frame_img_size = frame_img.size
    frame_img_resized = frame_img.resize((frame_img_size[0] / 2, frame_img_size[1] / 2), Image.ANTIALIAS)
    frame_img_resized.save(f)
    assert (frame_img_resized.size == (512, 640))
    logging.debug(('img {f} is halved to size {s}'.format(f=fullpath, s=frame_img_resized.size)))

def halve_imgs_in_dir(dr_path):
    test_frames=[]
    test_frames_this_dir=glob.glob(dr_path + '/*.jpg')
    test_frames_this_dir.sort()
    test_frames.extend(test_frames_this_dir)
    nframes_to_test=len(test_frames)

    for f in test_frames:
        halve_img(f)



def parse_args():
    parser=argparse.ArgumentParser('fetch tar files')
    default_rvm_path='rvm_name'
    parser.add_argument('-b', '--s3_input_bucket', help='s3_input_bucket', dest='s3_input_bucket', default='agridatadepot')
    parser.add_argument('-d', '--data_dir', help='data dir', dest='data_dir', default=r'C:\Users\Administrator\Desktop\videos')
    parser.add_argument('-r', '--rvm_name', help='rvm_name',  dest='rvm_name', required=True)
    parser.add_argument('-p', '--prefix_to_video_location', help='prefix_to_video_location',  dest='prefix_to_video_location', required=True)
    parser.add_argument('-v', '--video_num_limits', help='video num limits',  dest='video_num_limits', nargs=2, required=True)
    parser.add_argument('-s', '--status_filepath', help='status filepath', dest='status_filepath', required=True)
    parser.add_argument('-e', '--expected_videoname_re', help='expected_videoname_rgular expr', dest='expected_videoname_re', default='.*[.]tar[.]gz')
    parser.add_argument('-o', '--check_only', help='check_status_only', action='store_true',
                        dest='check_status_only')
    parser.add_argument('-m', '--misc', help='misc', action='store_true',
                        dest='misc')
    args=parser.parse_args()
    pprint((args))
    return args

def check_filesize_comparison(client, local_fullpath,  s3_bucket, s3_key_prefix):
    response = client.head_object(Bucket=s3_bucket, Key=s3_key_prefix)
    size_in_s3 = response['ContentLength']
    statinfo = os.stat(local_fullpath)
    if statinfo.st_size != size_in_s3:
        logging.error(
            ('local fullpath : %s, size %d,  size in s3: %d' % (local_fullpath, statinfo.st_size, size_in_s3)))
        return False
    return True

def down_load_file_from_s3_core(client, local_fullpath, s3_bucket, s3_key_prefix):

    with open(local_fullpath, 'wb') as data:
        try:
            client.download_fileobj(s3_bucket, s3_key_prefix, data)
        except Exception as e:
            logging.error(('There was a problem!', str(e)))
            raise
    if not os.path.isfile(local_fullpath):
        raise RuntimeError('video file %s not present' % local_fullpath)



    logging.debug(('video file %s downloaded succesfully' % local_fullpath))
    v_folderpath = os.path.splitext(os.path.splitext(local_fullpath)[0])[0]
    if os.path.isdir(v_folderpath):
        shutil.rmtree(v_folderpath)
    if os.path.isdir(v_folderpath):  # os.path.splitext(local_fullpath)):
        raise RuntimeError('untarred folder %s not supposed to present' % v_folderpath)
    logging.debug(('previously untarred dir %s was removed if present' % v_folderpath))

    tar = tarfile.open(local_fullpath)
    tar.extractall(path=v_folderpath)
    tar.close()

    if not os.path.isdir(v_folderpath):
        raise RuntimeError('cannot find untarred folder %s' % v_folderpath)

    logging.debug(('succesfully untarred  to %s' % v_folderpath))



def video_file_folder_it(args):

    video_names = []
    lims = [ int(i) for i in args.video_num_limits]
    #print lims
    expected_videoname_re=re.compile(args.expected_videoname_re, re.I)
    #with open('/Users/mypolopony/Downloads/row_video_map_may_15_16_ivory.csv', 'rbU') as f:
    with open(os.path.join(args.data_dir, args.rvm_name), 'rbU') as f:
        reader = csv.reader(f)
        count = 0
        for row in reader:
            if not count:
                count += 1
                continue
            else:
                m = expected_videoname_re.match(row[4])
                scan_id=row[9]
                if not m:
                    raise RuntimeError('video name: %s in csv file %s not in expected format' % (row[4], args.rvm_name))
                if not scan_id.strip():
                    raise RuntimeError('scanid:in csv file %s not in expected format' % ( args.rvm_name))
                video_names.append(scan_id + '/' + row[4])
                count += 1

    try:
        names =  list(set(video_names))[int(lims[0]):int(lims[1])]
    except:
        names = list(set(video_names))[int(lims[0]):int(len(list(set(video_names))))]

    for path_name in names:
        name=os.path.split(path_name)[1]
        v_fullpath=os.path.join(args.data_dir, name)
        s3_key_prefix=args.prefix_to_video_location + '/' +  path_name
        yield (v_fullpath, path_name)

def main(args):
    with infra_common.StatusWriter(args.status_filepath) as st_wr:
        args=parse_args()
        init_log(args)
        try:
            for v in video_file_folder_it(args):
                v_fullpath = v[0]
                s3_key_prefix=args.prefix_to_video_location + '/' +  v[1]
                if args.check_status_only:
                    b_comparison = check_filesize_comparison(client, v_fullpath, args.s3_input_bucket, s3_key_prefix)
                    if not b_comparison:
                        raise RuntimeError('%s not agreeing in size compared to its s3 counterpart' % (v_fullpath))
                    pass
                elif args.misc:
                    halve_imgs_in_dir(v_fullpath)
                else:
                    down_load_file_from_s3_core(client, v_fullpath, args.s3_input_bucket, s3_key_prefix)
        except StopIteration:
            pass


if __name__ == "__main__":
    args=parse_args()
    main(args)
    pass
