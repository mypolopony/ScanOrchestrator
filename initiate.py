from utils.models_min import Task
import numpy as np
import ConfigParser
import os
import yaml
import glob
import redis
import sys
import requests
import datetime
import traceback
from pprint import pprint
import binascii
import argparse
from utils import RedisManager
from utils.connection import *
from utils.models_min import *

# Load config file
config = ConfigParser.ConfigParser()
config_parent_dir = '.'
config_path = os.path.join(config_parent_dir, 'utils', 'poller.conf')
config.read(config_path)

# Redis queue
redisman = RedisManager(host=config.get('redis','host'), db=config.get('redis', 'db'), port=config.get('redis','port'))

def insert(task):
    pprint(task)
    redisman.put(':'.join([task['role'], task['session_name']]), task)


def parse_args():
    '''
    Add other parameters here
    '''
    parser=argparse.ArgumentParser('orchestrator')
    parser.add_argument('-t', '--task', help='taskfile', dest='taskfile', default=None)

    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = parse_args()

    # Task definition
    if args.taskfile:
        taskfiles = ['tasks/{}'.format(args.taskfile)]
    else:
        taskfiles = glob.glob('tasks/*.yaml')

    for taskfile in taskfiles:
        try:
            print('Reading task file: {}'.format(taskfile))
            task = yaml.load(open(taskfile, 'r'))
            task = Task(client_name=task['client_name'],
                        farm_name=task['farm_name'],
                        block_name=task['block_name'],
                        session_name=task['session_name'],
                        caffemodel_s3_url_cluster=task['detection_params']['caffemodel_s3_url_cluster'],
                        caffemodel_s3_url_trunk=task['detection_params']['caffemodel_s3_url_trunk'],
                        test=task['test'],
                        exclude_scans=task['exclude_scans'],
                        include_scans=task['include_scans'],
                        role=task['role']).to_json()

            # Add fields to generate process task
            # TODO: Turn responsibility for this over to Task object
            role = task['role']                 # Only one task for RVM
            if role == 'rvm':
                insert(task)

            # This can insert directly into process but is probably made obsolete by the repair script
            elif role == 'process':
                # Get the list of zips in detection folder
                base_url_path = '{}/results/farm_{}/block_{}/{}'.format(task.clientid, task.farm_name.replace(' ' ,''), task.block_name.replace(' ',''), task.session_name)
                zips = s3.list_objects(Bucket=config.get('s3','bucket'),Prefix='{}/detection'.format(base_url_path))
                zips = [z['Key'] for z in zips['Contents'] if '.zip' in z['Key']]

                # Task template
                task['num_retries'] = 0
                task['detection_params']['base_url_path'] = base_url_path
                task['detection_params']['bucket'] = config.get('s3','bucket')
                task['detection_params']['input_path'] = 'preprocess-frames'
                task['detection_params']['output_path'] = 'detection'
                task['detection_params']['session_name'] = datetime.datetime.now().strftime('%H-%M-%S')

                # Generate and insert the new tasks
                tasks = list()
                for z in zips:
                    newtask = task.copy()
                    newtask['detection_params']['folders'] = [os.path.basename(z)]
                    newtask['detection_params']['result'] = z
                    insert(newtask)

        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)