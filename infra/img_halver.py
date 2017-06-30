
from pprint import pprint, pformat

import os
import sys
import numpy as np
import time
import logging
import cv2
import glob
import argparse
import time
import zipfile
import imp
from pprint import pprint
from shutil import copyfile
import re




from PIL import Image

#relative path import
#mechanism to dynamically include the relative path where utility python modules are kept to the module search path.
from inspect import getsourcefile

current_path = os.path.abspath(getsourcefile(lambda:0))
parent_dir = os.path.split(os.path.dirname(current_path))[0]
sys.path.insert(0, parent_dir)
output_subdir='output'
source_dir=os.path.dirname(current_path)
output_rootdir=os.path.join(source_dir, output_subdir)

#local files, which are imported relatively
#caffemodel_name=os.path.join('trunk_detect_mar2017', 'trunk_detect_4_lr_1e6_wd_5e6_10000.caffemodel') #standard trunk detection
#caffemodel_name='train_iter_100000_prasad_cl_2_le_1e6.caffemodel' #the one that is 2-classes , prasad's
#caffemodel_name='trunk_detect_4_lr_1e6_wd_5e6_10000.caffemodel' #standard trunk detection
#caffemodel_name='train_iter_128000.caffemodel' #the one that is causing blue sky, paches problem
#caffemodel_name='train_foo_class_3_2_iter_42000.caffemodel' #the one with 3 classes 2nd attempt

#random seed
random_seed=42
g_key_val={}

def make_preprocess_xform_class_inst(filepath, expected_class):
    class_inst = None
    if not os.path.isfile(filepath):
        raise RuntimeError('expected file: %s not found' % (filepath))
    mod_name,file_ext = os.path.splitext(os.path.split(filepath)[-1])

    if file_ext.lower() == '.py':
        py_mod = imp.load_source(mod_name, filepath)

    elif file_ext.lower() == '.pyc':
        py_mod = imp.load_compiled(mod_name, filepath)

    if hasattr(py_mod, expected_class):
        class_inst = getattr(py_mod, expected_class)()

    return class_inst




def parse_ifile(args):
    r_key_val=re.compile('\#(?P<key>\w+)=(?P<val>\S+)')
    logging.debug(('parsing ifile', args.ifile_path))
    assert(args.ifile_path)
    zipfile_list=[]
    key_val={}
    with file(args.ifile_path) as fp:

        logging.debug(('parsing ifile again', args.ifile_path))
        for l in fp:
            logging.debug(('parsing ifile, line', l))
            l=l.strip()
            if not l:
                continue
            if l.startswith('#'):
                m=r_key_val.match(l)
                if m:
                    key_val[m.group('key')]=m.group('val')
                continue
            if l.endswith('.zip'):
                zipfile_list.append(l)
    logging.debug(('returning zipfilelist', ('%r'%zipfile_list)))
    return zipfile_list, key_val


def init(args):
    """
    start log
    """
    current_path = os.path.abspath(getsourcefile(lambda: 0))
    source_dir = os.path.dirname(current_path)
    parent_dir = os.path.split(os.path.dirname(current_path))[0]


    #start logger
    logger=logging.getLogger()
    logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    #fh = logging.FileHandler( os.path.join(output_dir, 'solve_log.txt'), mode='w')
    #fh.setLevel(logging.DEBUG)
    # create console handler with a higher log level
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)


    #logger.addHandler(fh)
    logger.addHandler(ch)
    logging.debug('Started')
    logging.debug('numpy version: %r' % (np.__version__))
    logging.debug((args))
    return




def process_core(args, unzipped_folder_names):
    """
        for detection of multiple frames residing in zip files or folders
    """
    def compute_output_folder_root(output_dir, bucket_name, folder_path):
        return os.path.join(output_dir, bucket_name, folder_path)






    test_frames=[]
    for d in unzipped_folder_names:
        d=os.path.join(args.folder_path, d)
        dname=os.path.split(d)[1]
        test_frames_this_dir=glob.glob(d + '/*.jpg')
        test_frames_this_dir.sort()
        test_frames.extend(test_frames_this_dir)


    nframes_to_test=len(test_frames)

    for f in test_frames:
        if False:
            continue
        else:
            dname= os.path.split(os.path.split(f)[0])[1]
            output_dir_for_this=os.path.join(args.folder_path, dname )
            frame_img=Image.open(f)
            frame_img_size=frame_img.size
            frame_img_resized = frame_img.resize((frame_img_size[0]/2, frame_img_size[1]/2), Image.ANTIALIAS)
            frame_img_resized.save(f)
            assert(frame_img_resized.size == (512, 640))
            logging.debug(('img {f} is halved to size {s}'.format(f=f,s=frame_img_resized.size)))




def main(args):
    init(args)
    unzipped_folder_name=os.path.split(args.folder_path)[1]
    args.folder_path=os.path.dirname(args.folder_path)
    unzipped_folder_names=[unzipped_folder_name]
    process_core(args,unzipped_folder_names)

def parse_args():
    parser=argparse.ArgumentParser('detection script')
    group = parser.add_mutually_exclusive_group()
    default_user_defined_preprocess_xform_filepath='user_defined_preprocess_xform.py'
    group.add_argument('-f', '--folder_path', help='file path to folder conaining jpg files', dest='folder_path')
    parser.add_argument('-s', '--session_name', help='name of this detection session', required=True, dest='session_name')


    args=parser.parse_args()
    pprint((args))
    return args


if __name__ == "__main__":
    args=parse_args()
    main(args)
    pass
