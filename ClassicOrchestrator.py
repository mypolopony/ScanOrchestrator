#!/usr/bin/python

import ConfigParser
import copy
import json
import logging
import sys
import time

import boto3
import numpy as np
from mongoengine import connect
from pymongo import MongoClient

# AgriData
from infra import do_instances_win
from models_min import *

# Database
server = 'ds161551.mlab.com'
port = 61551
username = 'agridata'
password = 'agridata'
dbname = 'dev-agdb'
connect(dbname, host=server, port=port, username=username, password=password)
c = MongoClient('mongodb://' + username + ':' + password + '@' + server + ':' + str(port) + '/' + dbname)

# Operational parameters
WAIT_TIME = 20  # AWS wait time for messages
NUM_MSGS = 10  # Number of messages to grab at a time
RETRY_DELAY = 60  # Number of seconds to wait upon encountering an error

# Load config file
config = ConfigParser.ConfigParser()
config.read('poller.conf')

# AWS Resources: S3
S3Key = config.get('s3', 'aws_access_key_id')
S3Secret = config.get('s3', 'aws_secret_access_key')
s3 = boto3.client('s3', aws_access_key_id=S3Key, aws_secret_access_key=S3Secret)

# AWS Resources: SQS
SQSKey = config.get('sqs', 'aws_access_key_id')
SQSSecret = config.get('sqs', 'aws_secret_access_key')
SQSQueueName = config.get('sqs', 'queue_name')
SQSQueueRegion = config.get('sqs', 'region')
sqsr = boto3.resource('sqs', aws_access_key_id=SQSKey, aws_secret_access_key=SQSSecret, region_name=SQSQueueRegion)
queue = sqsr.get_queue_by_name(QueueName='new-upload-queue')

# Initializing logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]: %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S')


class ArgsObject:
    def __init__(self, response):
        self.__dict__['_response'] = response

    def __getattr__(self, key):
        try:
            return self._response[key]
        except KeyError:
            sys.stderr.write('Sorry no key matches {}'.format(key))

    def __repr__(self):
        return json.dumps(self._response)

    def __str__(self):
        return json.dumps(self._response)


def keepDetectionAlive(args):
    detectors = dict()

    # While the status of the instances are not all Success
    while set([instance['Status'] for instance in args.instances]) != set(['Success']):
        for instance in args.instances:
            detectors[instance.id] = {'instance': instance, 'last_pct': 0.0}

            keys = s3.list_objects(Bucket='sunworld_file_transfer', Prefix='_'.join([instance.fullname, instance.attempt]))
            video = [v for v in keys if '.tar.gz' in v and 'out' not in keys]
            video_out =  [v for v in keys if '.tar.gz' in v and 'out' in keys]

            if video:
                detectors['last_pct'] = float(len(video_out)) / float(len(video))


def calculateInstanceDetails(prefix, num):
    instances = list()
    instance_template = {'prefix': prefix,
                         'fullname': None,
                         'status': None,
                         'task': None,
                         'attempt': None,
                         'number': num}
    attempt = 1
    # Generate instance information
    for n in range(num):
        # Find the most recent trial
        previous = s3.list_objects(Bucket='sunworld_file_transfer', Prefix=prefix + '_' + str(n) + '_', Delimiter='/')
        if 'CommonPrefixes' in previous.keys():
            previous = [str(p['Prefix'][:-1]) for p in previous['CommonPrefixes'] if 'out' not in p['Prefix']]
            last = sorted(previous, key=lambda x: int(x.split('_')[-1]), reverse=True)[0]
            attempt = np.max([int(last.split('_')[-1]) + 1, attempt])
        else:
            attempt = 1

    logging.info('Using Trial Number: {}'.format(attempt))

    for n in range(num):
        inst = copy.deepcopy(instance_template)
        inst['attempt'] = attempt
        inst['number'] = n
        inst['fullname'] = '_'.join([prefix, str(inst['number']), str(attempt)])
        instances.append(inst)

    return instances


def execute(task, args):
    '''
    Main Koshyframework entry
    
    :param task: Task to be executed
    :param args: Standard argument object
    :return: results contains instance level data
    '''

    args.stage = task
    args.check_status_only = False

    # TODO Make this a function with *kwargs
    logging.info('')
    logging.info('>> Executing {} <<'.format(task))
    logging.info('')

    results = do_instances_win.run(args)

    return results


def check(task, args):
    '''
    Koshyframework entry for tasks that require polling
    
    :param task: Task to be executed
    :param args: Standard argument object
    :return: results contains instance level data
    '''

    args.stage = task
    args.check_status_only = True

    success = None
    results = None

    while not success:
        time.sleep(60)
        logging.info('')
        logging.info('>> Status Check: {} <<'.format(task))

        results = do_instances_win.run(args)

        complete = [v for v in results if v['result'].lower() == 'success']
        incomplete = [v for v in results if v['result'].lower() != 'success']

        logging.info('Complete: {}'.format(complete))
        logging.info('Incomplete: {}'.format(incomplete))

        if len(incomplete) == 0:
            logging.info('Completed {}'.format(task))
            success = True

    return results


def run():
    '''
    This is an override in place of the long poller. Here, we can direct activity explicitly
    '''
    client = Client.objects.get(name='Duckhorn Vineyards')
    block = Block.objects.get(name='THPCF-08')
    farm = Farm.objects.get(id=block.farm)

    prefix = client.name.replace(' ','').lower() + '_' + block.name
    scanids = ['2017-06-26_11-15']
    num_instances = 2

    instances = calculateInstanceDetails(prefix, num_instances)
    print('Instances: {}'.format(instances))

    # Standard argument object
    args = ArgsObject({'session_name': datetime.now().strftime('%m-%d-%H-%M'),
                       'b_force': True,
                       'instances': dict(zip(range(num_instances), [inst['fullname'] for inst in instances])),
                       'prefix': prefix,
                       'clientid': str(client.id),
                       'clientname': client.name,
                       'farmid': str(farm.id),
                       'farmname': farm.name,
                       'blockid': str(block.id),
                       'blockname': block.name,
                       'scanids': scanids,
                       'upload_bucket': 'sunworld_file_transfer',
                       })

    # These processes require the extra check
    waitfor = ['rvm_generate', 'preprocess', 'shape_estimation', 'process', 'postprocess']

    # Task sequence
    sequence = [
        'restart',
        'matlab_kill',
        'clear_folder_and_sync_code',
        'git_pull_and_config',
        'pre_rvm_generate',
        'restart',
        'rvm_generate',
        'rvm_copy',
        'video_xfer',
        'restart',
        'preprocess',
        'detection',
        'check_detection_status',
        'post_detect_xfer',
        #'restart',
        #'shape_estimation',
        'restart',
        'process',
        'restart',
        'postprocess'
    ]



    # Main loop
    for stage in sequence:
        execute(stage, args)
        if stage in waitfor:
            check(stage, args)


if __name__ == '__main__':
    from infra.config_writer_win import main

    main(None)
    run()

