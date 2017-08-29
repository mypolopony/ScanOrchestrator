import os
import sys
import ConfigParser
import datetime
import time
import json
import argparse
import re
import subprocess
import azurerm
from pprint import pprint
from kombu import Connection

# Tasks
roles = ['rvm', 'preprocess', 'detection', 'process']

# Load config file
config = ConfigParser.ConfigParser()
config_parent_dir = '.'

if os.name == 'nt':
    config_parent_dir = r'C:\AgriData\Projects\ScanOrchestrator'
    import matlab.engine
else:
    assert (os.path.basename(os.getcwd()) == 'ScanOrchestrator')

config_path = os.path.join(config_parent_dir, 'utils', 'poller.conf')
assert (os.path.isfile(config_path))
config.read(config_path)

# ANSI escape (to remove color codes from subprocess output)
ansi_escape = re.compile(r'\x1b[^m]*m')

# Azure connection (keys = ['tenant', 'tokenType', 'expiresOn', 'accessToken', 'subscription'])
auth = subprocess.check_output(['az','account','get-access-token'])
auth = json.loads(ansi_escape.sub('', auth))

def sendtoKombu(queue, task):
    with Connection('amqp://{}:{}@{}:5672//'.format(config.get('rmq', 'username'),
                                                    config.get('rmq', 'password'),
                                                    config.get('rmq', 'hostname'))) as kbu:
        q = kbu.SimpleQueue(queue)
        q.put(task)
        q.close()

def parse_args():
    parser=argparse.ArgumentParser('initiate')
    parser.add_argument('-s', '--session_name', help='session_name', dest='session_name',required=True)
    args = parser.parse_args()
    return args


def main(args):
    # Task definition - Start with RVM
    task = {
        'clientid': '591daa81e1cf4d8cbfbb1bf6',
        'farmname': 'Quatacker-Burns',
        'scanids': ['2017-08-17_09-47','2017-08-17_10-25','2017-08-17_11-23','2017-08-17_12-07','2017-08-17_12-33'],
        'blockname': '4D',
        'role': 'rvm',
        'session_name': args.session_name,
        'test': False,
        'detection_params': {
            'caffemodel_s3_url_cluster': 's3://deeplearning_data/models/best/cluster_june_15_288000.caffemodel',
            'caffemodel_s3_url_trunk': 's3://deeplearning_data/models/best/trunk_june_10_400000.caffemodel'
        }
    }

    # Explicitly create the queues (is this necessary?)
    with Connection('amqp://{}:{}@{}:5672//'.format(config.get('rmq', 'username'),
                                                    config.get('rmq', 'password'),
                                                    config.get('rmq', 'hostname'))) as kbu:
        for role in roles:
            simple_queue = kbu.SimpleQueue('_'.join([role, args.session_name]))
            simple_queue.close()

    sendtoKombu('%s_%s' % (task['role'], task['session_name']), task)


if __name__ == '__main__':
    args = parse_args()
    main(args)
