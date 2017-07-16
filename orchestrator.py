#!/usr/bin/python

import ConfigParser
import json
import logging
import subprocess
import tarfile
import glob
import sys
import os
import re
import time
import boto3
import shutil

import pandas as pd
import numpy as np

from bson.objectid import ObjectId

# AgriData
from utils.models_min import *
from utils.connection import *

# Operational parameters
WAIT_TIME = 20  # AWS wait time for messages
NUM_MSGS = 10  # Number of messages to grab at a time
RETRY_DELAY = 60  # Number of seconds to wait upon encountering an error

# Load config file
config = ConfigParser.ConfigParser()
if os.name == 'nt':
    config.read('E:\\Projects\\ScanOrchestrator\\utils\\poller.conf')
else:
    config.read('utils/poller.conf')

# Temporary location for collateral in processing
tmpdir = config.get('env', 'tmpdir')
if not os.path.exists(tmpdir):
    os.makedirs(tmpdir)

# AWS Resources: S3
S3Key = config.get('s3', 'aws_access_key_id')
S3Secret = config.get('s3', 'aws_secret_access_key')
s3 = boto3.client('s3', aws_access_key_id=S3Key, aws_secret_access_key=S3Secret)
s3r = boto3.resource('s3', aws_access_key_id=S3Key, aws_secret_access_key=S3Secret)

# AWS Resources: SQS
SQSKey = config.get('sqs', 'aws_access_key_id')
SQSSecret = config.get('sqs', 'aws_secret_access_key')
SQSQueueName = config.get('sqs', 'queue_name')
SQSQueueRegion = config.get('sqs', 'region')
sqsr = boto3.resource('sqs', aws_access_key_id=SQSKey, aws_secret_access_key=SQSSecret, region_name=SQSQueueRegion)
queue = sqsr.get_queue_by_name(QueueName=SQSQueueName)

# AWS Resources: SNS
aws_arns = dict()
aws_arns['statuslog'] = 'arn:aws:sns:us-west-2:090780547013:statuslog'
sns = boto3.client('sns', region_name=config.get('sns','region'))

# Azure Service Bus
from azure.servicebus import ServiceBusService, Message, Queue
bus_service = ServiceBusService(service_namespace='agridataqueues',
                                shared_access_key_name='sharedaccess',
                                shared_access_key_value='cWonhEE3LIQ2cqf49mAL2uIZPV/Ig85YnyBtdb1z+xo=')


# Initialize logging
logger = logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]: %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S')
logger = logging.getLogger('default')
logger.setLevel(logging.DEBUG)
# File Handler
fh = logging.FileHandler(config.get('file_logger', 'log_path'))
fh.setLevel(logging.DEBUG)
# Console Handler
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
# Formatter
formatter = logging.Formatter('%(asctime)s [%(levelname)s]: %(message)s', datefmt='%a, %d %b %Y %H:%M:%S')
ch.setFormatter(formatter)
fh.setFormatter(formatter)
# Add handlers
logger.addHandler(ch)
logger.addHandler(fh)

def announce(func, *args, **kwargs):
    def wrapper(*args, **kwargs):
        logger.info('***** Executing {} *****'.format(func.func_name))
        return func(*args, **kwargs)

    return wrapper

@announce
def report(args, results):
    payload = list()

    '''
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

@announce
def handleAWSMessage(result):
    '''
    This is a basic AWS message handler that can be extended for a more specialized task. It likely won't be used in
    the transition to Azure except perhaps to shuttle messages from one service to the other.
    '''

    # Weird double string to JSON
    msg = json.loads(json.loads(result.body)['Message'])['Records'][0]
    obj = msg['s3']['object']

    # Split key into relevant types
    task = dict()
    task['clientid']    = obj['key'].split('/')[0]
    task['scanid']      = obj['key'].split('/')[1]
    task['filename']    = obj['key'].split('/')[2]
    task['size']        = float(obj['size']) / 1024.0

    logger.info('\tWorking on {} ({} MB)'.format(task['filename'], task['size']))

    for goodkey in ['clientid', 'scanid', 'filename', 'size']:
        logger.info('\t{}\t{}'.format(goodkey.capitalize(), task[goodkey]))

        # Delete task / Re-enque task on fail
        # When tasks are received, they are temporarily marked as 'taken' but it is up to this process to actually
        # delete them or, failing that, default to releasing them back to the queue

@announce
def poll():
    start = datetime.datetime.now()

    logger.info('Amazon AWS SQS Poller\n\n')

    # Poll messages
    # while True:
    try:
        logger.info('Requesting tasks')
        try:
            results = queue.receive_messages(MaxNumberOfMessages=NUM_MSGS, WaitTimeSeconds=WAIT_TIME)
        except Exception as e:
            logger.error('A problem occured: {}'.format(str(e)))
            raise e

        # For each result. . .
        logger.info('Received {} tasks'.format(len(results)))
        for ridx, result in enumerate(results):
            try:
                logger.info('Handling message {}/{}'.format(ridx, len(results)))
                handleAWSMessage(result)
            except:
                logger.exception('Error while handling messge: {}'.format(result.body))
                # can add dead letter queue for poisonous messages here
        else:
            logger.info('No tasks to do.')

    except Exception as e:
        # General errors, with a retry delay
        logger.exception('Unexpected error: {}. Sleeping for {} seconds'.format(str(e), RETRY_DELAY))

    logger.info('Sleeping for {} seconds'.format(RETRY_DELAY))
    time.sleep(RETRY_DELAY)


@announce
def transformScan(scan):
    '''
    This is a method for changing old-style scanids into new-style scanids. This will eventually become
    deprecated as we switch (on the boxes) to the new format. It is called for old-style scans before
    they are processed.

    To do this:
        1) Add the scan ID to the .tar.gz file (this is useful when working with multiple scans)
            - Rename the files on S3 by moving the originals to the new style with formatting change
            - Delete the old files (this is safe after several weeks of code verification; also, backups exist)
        2) Fill in the 'filename' column of the CSVs which can be mysteriously missing
        3) Rename the CSVs by placing the scanid before the cameraid
        4) Re-upload
    '''

    logger.info('Transforming Scan {} (client {})'.format(scan.client, scan.scanid))

    ## Rename tars and csvs in place on S3
    # This can be done without downloading any of the .tar.gz files, although they will
    # be downloaded later. This process is expected to take some time, as resource on S3
    # cannot be moved but must be copied.
    pattern_cam = re.compile('[0-9]{8}')
    for fidx, file in enumerate(s3.list_objects(Bucket=config.get('s3','bucket'),
                                                Prefix='{}/{}'.format(scan.client, scan.scanid))['Contents']):
        # Exclude the folder itself and any good files (sorry 2018)
        if file['Key'] != '{}/{}/'.format(str(scan.client), str(scan.scanid)) and not file['Key'].startswith('2017'):     
            # Extract camera and timestamp for rearrangement
            camera = re.search(pattern_cam, file['Key']).group()
            time = '_'.join([file['Key'].split('_')[-2], file['Key'].split('_')[-1]])

            # Tars
            if '.tar.gz' in file['Key']:
                newfile = '{}/{}/{}_{}_{}'.format(str(scan.client), str(scan.scanid), str(scan.scanid), camera, time)

            # CSVs
            if '.csv' in file['Key']:
                newfile = '{}/{}/{}_{}.csv'.format(str(scan.client), str(scan.scanid), str(scan.scanid), camera)

            logger.info('Renaming {} --> {}'.format(file['Key'], newfile))

            # Copy and Delete
            s3r.Object(config.get('s3', 'bucket'), newfile).copy_from(CopySource={'Bucket': config.get('s3', 'bucket'),
                                                                                      'Key': file['Key']})

            # Tthe Great Rename Fiasco of 2017 was brought about by this line. As files in the old format were deleted,
            # boxes in the field began to began to re-upload imagery, starting with the earliest scans. The line is kept
            # here as an historical exhibit.
            # s3r.Object(config.get('s3', 'bucket'), file['Key']).delete()


    ## Download files required for tar inspection and cv update
    # Temporary file destination
    dest = os.path.join(tmpdir, str(scan.client), str(scan.scanid))
    if not os.path.exists(dest):
        os.makedirs(dest)

    # Rsync scan to temporary folder
    subprocess.call(['rclone',
                     '--config', '{}/.config/rclone/rclone.conf'.format(os.path.expanduser('~')),
                     'copy', '{}:{}/{}/{}'.format(config.get('rclone','profile'),
                                                   config.get('s3','bucket'),
                                                   str(scan.client),
                                                   str(scan.scanid)), dest])

    ## Add filenames column to CSV
    logger.info('Filling in CSV columns. . . this can take a while. . .')
    for csv in glob.glob(dest + '/*.csv'):
        camera  = re.search(pattern_cam, csv).group()
        log     = pd.read_csv(csv)
        try:
            names = list()
            for tf in sorted(glob.glob(dest + '/*' + camera + '*.tar.gz')):
                for i in range(0,len(tarfile.open(tf).getmembers())):
                    names.append(tf.split('/')[-1])

            if len(names) == len(log) + 1:
                logger.info('Names == Log + 1: This is normal, shaving one from Names')
                names = names[:-1]
            elif len(names) == len(log) - 1:
                logger.info('Log == Names + 1: This is odd but still okay, adding one to Names')
                names.append(names[-1])

            log['filename'] = names
            log.to_csv(csv)
        except Exception as e:
            logger.error('\n *** Failed: {} {}'.format(camera, csv))
            continue

    ## Upload
    subprocess.call(['rclone', '-v',
                     '--config', '{}/.config/rclone/rclone.conf'.format(os.path.expanduser('~')),
                     'copy', dest,
                     '{}:{}/{}/{}/'.format(config.get('rclone', 'profile'),
                                                   config.get('s3', 'bucket'),
                                                   str(scan.client),
                                                   str(scan.scanid))])

    ## Delete from local
    # Using 'dest' explicitly will delete the scan folder but not the client folder. Here, we
    # recreate the base temporary directory
    shutil.rmtree(os.path.join(tmpdir, str(scan.client)))


@announce
def initiateScanProcess(scan):
    '''
    This is the entry point to the processing pipeline. It takes a raw, newly uploaded scan and
    handles some essential tasks that are required before processing.
    '''

    ## Check for formatting (old style -> new style)
    # These are the keys (files) in the scan directory
    s3base = '{}/{}'.format(str(scan.client), str(scan.scanid))
    keys = [obj['Key'] for obj in s3.list_objects(Bucket=config.get('s3','bucket'), Prefix=s3base)['Contents']]

    # If there are any files that don't start with a scanid, they must be of the old format, so format them.
    if not len([k for k in keys if k.split('/')[-1].startswith(scan.scanid)]):
        transformScan(scan)
    # print('This feature disabled; a more intelligent service is required to perform transformation')

    # The scan is uploaded and ready to be placed in the 'ready' or 'rvm' queue, where it
    # may be be evaluated for 'goodness'

@announce
def sendtoServiceBus(queue, msg):
    '''
    This is plainly just a wrapper for a message to a service bus, maybe other messaging things will happen here
    '''
    bus_service.send_queue_message(queue, Message(msg))


@announce
def receivefromServiceBus(queue, lock=False):
    '''
    A generic way of asking for work to do, based on the task. The default behavior, for longer running processes,
    is for peek_lock to be False, which means deleting the message when it is received. This means that the
    task process is responsible for re-enqueing the task if it fails.
    '''
    incoming = bus_service.receive_queue_message(queue, peek_lock=lock)

    # Here, we can accept either properly formatted JSON strings or, if necessary, convert the
    # quotes to make them compliant.
    try:
        msg = json.loads(incoming.body)
    except:
        msg = json.loads(incoming.body.replace("'",'"'))

    return msg

@announce
def emitSNSMessage(message, context=None, topic='statuslog'):
    # A simple notification payload, sent to all of a topic's subscribed endpoints
    payload = {
        'message'   : message,
        'topic_arn' : aws_arns[topic],
        'context'   : context
    }

    # Emit the message
    response = sns.publish(
        TopicArn    =  aws_arns[topic],
        Message     =  json.dumps(payload),                 # SNS likes strings
        Subject     =  '{} message'.format(topic)
    )


@announce
def generateRVM(task):
    '''
    Given a set of scans (or one scan), generate a row video map.
    '''

    # Obtain the scan
    scan = Scan.objects.get(client=ObjectId(task['clientid']), scanid=task['scanid'])
    scans = [str(scan.scanid)]

    # Check staging database for previous scans of this block
    staged = db.staging.find({'block': scan.blocks[0]})
    if staged:
        scans = scans + [s['_id'] for s in staged]

    # Start MATLAB
    mlab = matlabProcess()
    try:
        # Send the arguments off to batch_auto, return is the S3 location of rvm.csvs
        s3uri, localuri = mlab.runTask('rvm', task['clientid'], scans)
        data = pd.read_csv(localuri, header=0)
        rows_found = len(set([(r, d) for r,d in zip(data['rows'],data['direction'])])) / 2

        # Check for completeness
        try:
            block = Block.objects.get(id=scan.blocks[0])
            if rows_found < block.num_rows * 0.5:
                logger.warning('RVM is not long enough! Saving to holding area')
                # TODO: Emit message, insert into staging db
            if rows_found > block.num_rows:
                logger.warning('Too many rows found!')
                # TODO: Emit message
        except:
            pass

        # Split tarfiles
        tarfiles = pd.Series.unique(data['file'])
        for shard in np.array_split(tarfiles, int(len(tarfiles)/6)):
            task['tarfiles'] = list(shard)
            sendtoServiceBus('preprocess', task)

        emitSNSMessage('all is well!')
    except Exception as err:
        task['message'] = str(err)
        emitSNSMessage('Failure on {}'.format(json.dumps(task)))
        # TODO: Re-enqueue message
        # TODO: Place scans into staging
        return

    # Return characteristics of RVM, like goodness for example
    return None


@announce
def preprocess():
    '''
    Preprocessing method
    '''
    # Start MATLAB
    mlab = matlabProcess()

    # Grab a task
    task = receivefromServiceBus('preprocess')
    print(task)

    # Canonical filepath
    video_dir = r'E:\Projects\videos'

    while task:
        # Download the tarfiles
        for tar in task['tarfiles']:
            key = '{}/{}/{}'.format(task['clientid'], task['scanid'], tar)
            print(key)
            s3r.Bucket(config.get('s3','bucket').download_file(key, video_dir))

        # Run
        ret = mlab.runTask('preprocess', task['clientid'], scans)

@announce
def detection(scan):
    '''
    Detection method
    '''

    # Start MATLAB
    mlab = matlabProcess()


@announce
def process(scan):
    '''
    Processing method
    '''

    # Start MATLAB
    mlab = matlabProcess()


@announce
def identifyRole():
    '''
    This wmethod is called when an instance starts. Based on its ComputerInfo, it will identify itself as a
    member of a particular work group and then proceeded to execute the correct tasks.
    '''

    # Windows box
    if os.name == 'nt':
        try:
            # Bring in MATLAB
            import matlab.engine

            # Look for computer type (role)
            output = subprocess.check_output(["powershell.exe", "Get-ComputerInfo"], shell=True)
            instance_type = re.search('CsName[ ]+: [a-z]+', output).group().split(':')[-1].strip()
            return instance_type
        except Exception as e:
            print('Error: {}'.format(e))

    # Linux box
    else:
        return os.name


@announce
def matlabProcess(startpath=r'E:\Projects'):
    '''
    Start MATLAB engine. This should not be global because it does not apply to all users of the script. Having said that,
    my hope is that it becomes a pain to pass around. The Windows path is a safe default that probably should be offloaded
    elsewhere since it
    '''
    logger.info('Starting MATLAB. . .')
    mlab = matlab.engine.start_matlab()
    mlab.addpath(mlab.genpath(startpath))

    return mlab


if __name__ == '__main__':
    # Initial decision points. It is important that these do not return anything. This requires that each task stream
    # be responsible for handling its own end conditions, whether it be an graceful exit, an abrupt termination, etc. THe
    # reason is that that task itself has the sole responsibility of knowing what it should or should not be doing and how
    # to handle adverse or successful events.

    # Convert scan filenames and CSVs from old style to new style
    if sys.argv[1] == 'convert':
        # Scanid can be specified on the command line
        if len(sys.argv) == 3:
            scans = [Scan.objects.get(scanid=sys.argv[2])]

        # Or else we'll just run through all the scans
        else:
            scans = Scan.objects()

        for scan in scans:
            try:
                initiateScanProcess(scan)
            except Exception as e:
                logger.info('A fnord error has occured: {}'.format(e))

    # Daemon mode
    elif sys.argv[1] == 'poll':
        poll()

    # RVM Generation
    elif sys.argv[1] == 'rvm':
        task = {
           'clientid'    : '5953469d1fb359d2a7a66287',
           'scanid'      : '2017-06-30_10-01',
           'role'        : 'rvm',
        }
        logger.info('Initializing with scan {}'.format(task['scanid']))

    # Preprocessing
    elif sys.argv[1] == 'preprocess':
        preprocess()

    # Error
    else:
        logger.error('Sorry, no arguments supplied')