import os
import sys
import ConfigParser
import datetime
import time
import json
import argparse
import re
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

def sendtoKombu(queue, task):
    with Connection('amqp://{}:{}@{}:5672//'.format(config.get('rmq', 'username'),
                                                    config.get('rmq', 'password'),
                                                    config.get('rmq', 'hostname'))) as kbu:
        q = kbu.SimpleQueue(queue)
        q.put(task)
        q.close()


def parse_args():
    parser = argparse.ArgumentParser('initiate')
    parser.add_argument('-s', '--session_name', help='session_name', dest='session_name', required=True)
    args = parser.parse_args()
    return args


def main(args):
    # Task definition - Start with RVM
    task = {
        'clientid': '5953469d1fb359d2a7a66287',
        'farmname': 'Three Palms',
        'scanids': ['2017-07-20_10-03'],
        'blockname': 'THPME-10',
        'role': 'rvm',
        'session_name': args.session_name,
        'test': False
    }

    sendtoKombu('%s_%s' % (task['role'], task['session_name']), task)


if __name__ == '__main__':
    args = parse_args()
    main(args)
