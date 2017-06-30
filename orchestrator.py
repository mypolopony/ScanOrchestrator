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


'''
def report(args, results):
    payload = list()

    for result in results:
        item = {'instanceId': db.StringField(),
                'tagIndex' : db.IntegerField(),
                'tagName = db.StringField(),
                progress = db.FloatField(),
                status = db.StringField(),
                startTime = db.DateTimeField(),
                lastUpdatedAt = db.DateTimeField(),
                lastStateChange = db.DateTiemfielde(),
                task = db.StringField(),
                subtask = db.StringField(),
                currentCommandId = db.StringField()}
'''


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


def handleMessage(result):
    # Weird double string to JSON
    msg = json.loads(json.loads(result.body)['Message'])['Records'][0]
    obj = msg['s3']['object']

    # Split key into relevant types
    task = dict()
    task['clientid'] = obj['key'].split('/')[0]
    task['scanid'] = obj['key'].split('/')[1]
    task['filename'] = obj['key'].split('/')[2]
    task['size'] = float(obj['size']) / 1024.0

    logging.info('\tWorking on {} ({} MB)'.format(task['filename'], task['size']))

    for goodkey in ['clientid', 'scanid', 'filename', 'size']:
        logging.info('\t{}\t{}'.format(goodkey.capitalize(), task[goodkey]))

        # Delete task / Re-enque task on fail
        # When tasks are received, they are temporarily marked as 'taken' but it is up to this process to actually
        # delete them or, failing that, default to releasing them back to the queue


def poll():
    start = datetime.datetime.now()

    logging.info('Amazon AWS SQS Poller\n\n')

    # Poll messages
    # while True:
    try:
        logging.info('Requesting tasks')
        try:
            results = queue.receive_messages(MaxNumberOfMessages=NUM_MSGS, WaitTimeSeconds=WAIT_TIME)
        except Exception as e:
            logging.error('A problem occured: {}'.format(str(e)))
            raise e

        # For each result. . .
        logging.info('Received {} tasks'.format(len(results)))
        for ridx, result in enumerate(results):
            try:
                logging.info('Handling message {}/{}'.format(ridx, len(results)))
                handleMessage(result)
            except:
                logging.exception('Error while handling messge: {}'.format(result.body))
                # can add dead letter queue for poisonous messages here
        else:
            logging.info('No tasks to do.')

    except Exception as e:
        # General errors, with a retry delay
        logging.exception('Unexpected error: {}. Sleeping for {} seconds'.format(str(e), RETRY_DELAY))

    logging.info('Sleeping for {} seconds'.format(RETRY_DELAY))
    time.sleep(RETRY_DELAY)


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

    prefix = 'orchestrator_trial'
    num_instances = 2

    instances = calculateInstanceDetails(prefix, num_instances)
    print('Instances: {}'.format(instances))

    # Standard argument object
    args = ArgsObject({'session_name': datetime.strftime(datetime.now(), '%c'),
                       'b_force': True,
                       'instances': dict(zip(range(num_instances), [inst['fullname'] for inst in instances])),
                       'prefix': prefix,
                       'scan_folder': '59055036037c2fc5e372ad9d',
                       'upload_bucket': 'sunworld_file_transfer',
                       'expected_prefix': '22005520_2017-05-13'})

    # These processes require the extra check
    waitfor = ['rvm_generate', 'preprocess', 'process', 'postprocess']

    # Task sequence
    sequence = ['restart', 'matlab_kill', 'clear_folder_and_sync_code', 'git_pull_and_config', 'restart',
                'rvm_generate', 'video_xfer', 'restart', 'preprocess', 'detection', 'check_detection_status',
                'post_detect_xfer', 'restart', 'process', 'restart', 'postprocess']
    sequence = ['detection', 'check_detection_status',
                'post_detect_xfer', 'restart', 'process', 'restart', 'postprocess']

    # Main loop
    for stage in sequence:
        execute(stage, args)
        if stage in waitfor:
            check(stage, args)


if __name__ == '__main__':
    # poll()

    from infra.config_writer_win_test import main

    main(None)
    run()

'''
# :: Appendix A
# An example task:

{
  "Type" : "Notification",
  "MessageId" : "7115cdd0-e31f-5e43-9153-9c1221f88f32",
  "TopicArn" : "arn:aws:sns:us-west-2:090780547013:new-upload",
  "Subject" : "Amazon S3 Notification",
  "Message" : "{'Records':[
        {'eventVersion':'2.0',
         'eventSource':'aws:s3',
         'awsRegion':'us-west-2',
         'eventTime':'2017-06-20T06:28:55.494Z',
         'eventName':'ObjectCreated:Put',
         'userIdentity':
            {'principalId':'AWS:AIDAJPMALFTJJN5RTRQAC'},
         'requestParameters':
            {'sourceIPAddress':'98.210.157.168'},
         'responseElements':
            {'x-amz-request-id':'BB6CD5D54D02C197',
             'x-amz-id-2':'ZHly6fPy5asEZg5kamHhV6LNji7/T3Nn02InIE0/0QixAfDISj/Da5dtE66dzAzNH3DJ9SqvPmk='},
         's3':
            {'s3SchemaVersion':'1.0',
             'configurationId':'new_upload',
             'bucket':
                {'name':'agridatadepot',
                 'ownerIdentity': {'principalId':'A3GBGXVKMUOGDW'},
                 'arn':'arn:aws:s3:::agridatadepot'},
             'object':
                {'key':'OfficeBox/2017-99-99_99-99/2017-99-99_99-99.tar.gz',
                 'size':9069,
                 'eTag':'e2ce2b566d95a2b5f10d5570c501d4c7',
                 'sequencer':'005948C0A745CBCF86'}}}]}",
  "Timestamp" : "2017-06-20T06:28:55.566Z",
  "SignatureVersion" : "1",
  "Signature" : "lBabeX5mPNZDU3bfrOznWBeT5QuSQUaJCUCujkGMZhQeby7z1/9f7HdKDpYsdKjzL3bv0bJWkNDGfsyl9YmHAP++HllVxDo3ZXw8wnx8a1dYwVcH3iI5qbswywrWZSc28cY+L0JSTusc8x2ICpIBXHjhosAyoqBHlgBqtQ6KBj9WYXrjEA7Azs/WVDZhG357yzPK2GFuN8THtX7wWXssHlIp1gVoHb/IJD7Ii5KPFlq2DLJlFSqINYxY0UBoKr2m5e/nYGYri1NKdEh+Phkapi3gAub3nH6JJhfPZ+zHapDhxsrjD4SHC+dUBmOOv3nhEnHUDCp1Q316mH41Ihe7bQ==",
  "SigningCertURL" : "https://sns.us-west-2.amazonaws.com/SimpleNotificationService-b95095beb82e8f6a046b3aafc7f4149a.pem",
  "UnsubscribeURL" : "https://sns.us-west-2.amazonaws.com/?Action=Unsubscribe&amp;SubscriptionArn=arn:aws:sns:us-west-2:090780547013:new-upload:aa8b4d08-a7bd-4acb-963c-458b71512c1c"
}
'''
