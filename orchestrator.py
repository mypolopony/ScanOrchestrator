#!/usr/bin/python

import ConfigParser
import json
import logging
import subprocess
import tarfile
import glob
import os
import re
import time
import boto3
import shutil

import pandas as pd
import matlab.engine

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
config.read('utils/poller.conf')

# Temporary location for collateral in processing
tmpdir = config.get('env', 'tmpdir')
if not os.path.exists(tmpdir):
    os.mkdirs(tmpdir)

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
queue = sqsr.get_queue_by_name(QueueName='new-upload-queue')

# Azure Service Queue
from azure.servicebus import ServiceBusService, Message, Queue
bus_service = ServiceBusService(service_namespace='agridataschoolbus',
                                shared_access_key_name='SharedPolicy',
                                shared_access_key_value='+fWawVsNi4aD20AGFgxFvjoqOXd5tsGJsanieTDc+sI=')

# Initializing logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]: %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S')


def announce(func, *args, **kwargs):
    def wrapper(*args, **kwargs):
        logging.info('***** Executing {} *****'.format(func.func_name))
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

    logging.info('\tWorking on {} ({} MB)'.format(task['filename'], task['size']))

    for goodkey in ['clientid', 'scanid', 'filename', 'size']:
        logging.info('\t{}\t{}'.format(goodkey.capitalize(), task[goodkey]))

        # Delete task / Re-enque task on fail
        # When tasks are received, they are temporarily marked as 'taken' but it is up to this process to actually
        # delete them or, failing that, default to releasing them back to the queue

@announce
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

    logging.info('Transforming Scan {} (client {})'.format(scan.client, scan.scanid))

    ## Rename tars and csvs in place on S3
    # This can be done without downloading any of the .tar.gz files, although they will
    # be downloaded later. This process is expected to take some time, as resource on S3
    # cannot be moved but must be copied.
    pattern_cam = re.compile('[0-9]{8}')
    for fidx, file in enumerate(s3.list_objects(Bucket=config.get('s3','bucket'),
                                                Prefix='{}/{}'.format(scan.client, scan.scanid))['Contents']):
        if file['Key'] != '{}/{}/'.format(str(scan.client), str(scan.scanid)):     # Exclude the folder itself
            # Extract camera and timestamp for rearrangement
            camera = re.search(pattern_cam, file['Key']).group()
            time = '_'.join([file['Key'].split('_')[-2], file['Key'].split('_')[-1]])

            # Tars
            if '.tar.gz' in file['Key']:
                newfile = '{}/{}/{}_{}_{}'.format(str(scan.client), str(scan.scanid), str(scan.scanid), camera, time)

            # CSVs
            if '.csv' in file['Key']:
                newfile = '{}/{}/{}_{}.csv'.format(str(scan.client), str(scan.scanid), str(scan.scanid), camera)

            logging.info('Renaming {} --> {}'.format(file['Key'], newfile))

            # Copy and Delete
            s3r.Object(config.get('s3', 'bucket'), newfile).copy_from(CopySource={'Bucket': config.get('s3', 'bucket'),
                                                                                      'Key': file['Key']})
            s3r.Object(config.get('s3', 'bucket'), file['Key']).delete()


    ## Download files required for tar inspection and cv update
    # Temporary file destination
    dest = os.path.join(tmpdir, str(scan.client), str(scan.scanid))
    if not os.path.exists(dest):
        os.makedirs(dest)

    # Rsync scan to temporary folder
    subprocess.call(['rclone',
                     '--config', '{}/.config/rclone/rclone.conf'.format(config.get('env','home')),
                     'copy', '{}:{}/{}/{}'.format(config.get('rclone','profile'),
                                                   config.get('s3','bucket'),
                                                   str(scan.client),
                                                   str(scan.scanid)), dest])

    ## Add filenames column to CSV
    logging.info('Filling in CSV columns. . . this can take a while. . .')
    for csv in glob.glob(dest + '/*.csv'):
        camera  = re.search(pattern_cam, csv).group()
        log     = pd.read_csv(csv)
        try:
            names = list()
            for tf in sorted(glob.glob(dest + '/*' + camera + '*.tar.gz')):
                for i in range(0,len(tarfile.open(tf).getmembers())):
                    names.append(tf.split('/')[-1])

            if len(names) == len(log) + 1:
                logging.info('Names == Log + 1: This is normal, shaving one from Names')
                names = names[:-1]
            elif len(names) == len(log) - 1:
                logging.info('Log == Names + 1: This is odd but still okay, adding one to Names')
                names.append(names[-1])

            log['filename'] = names
            log.to_csv(csv)
        except Exception as e:
            logging.error('\n *** Failed: {} {}'.format(camera, csv))
            continue

    ## Upload and delete old (accomplished in one via rsync)
    subprocess.call(['rclone', '-v',
                     '--config', '{}/.config/rclone/rclone.conf'.format(config.get('env', 'home')),
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

    # The scan is uploaded and ready to be placed in the 'ready' or 'rvm' queue, where it
    # may be be evaluated for 'goodness'

@announce
def sendtoServiceBus(queue, msg):
    '''
    This is plainly just a wrapper for a message to a service bus, maybe other messaging things will happen here
    '''
    bus_service.send_queue_message(queue, Message(msg))


@announce
def receivefromServiceBus(role, lock=False):
    '''
    A generic way of asking for work to do, based on the task. The default behavior, for longer running processes,
    is for peek_lock to be False, which means deleting the message when it is received. This means that the
    task process is responsible for re-enqueing the task if it fails.
    '''
    qname = 'worker-' + role
    msg = bus_service.receive_queue_message(qname, peek_lock=lock)
    return msg.body


@announce
def generateRVM(task):
    '''
    Given a set of scans (or one scan), generate a row video map.
    '''

    # Obtain the scan
    scans = Scan.objects(client=task['clientid'], scanid=task['scanid'])

    # Check staging database for previous scans of this block
    staged = db.staging.find({'block': scan.blocks[0]})
    if staged:
        scans = scans + staged

    # Start MATLAB
    mlab = matlabProcess()
    try:
        # Send the arguments off to batch_auto
        ret = mlab.batch_auto(0, 1, 0, scans[0].client, scans)      # TODO: Beware these magic numbers
        logging.info('>> Success on {}'.format(scanid))
        # TODO: Emit message
    except:
        logging.error('>> Failure on {}'.format(scanid))
        # TODO: Emit message
        # TODO: Re-enqueue message
        # TODO: Place scans into staging
        return

    # Return characteristics of RVM, like goodness for example
    return None


@announce
def preprocess(scan):
    '''
    Preprocessing method
    '''

    # Start MATLAB
    mlab = matlabProcess()


@announce
def identifyRole():
    '''
    This wmethod is called when an instance starts. Based on its ComputerInfo, it will identify itself as a
    member of a particular work group and then proceeded to execute the correct tasks.
    '''

    if os.name == 'nt':
        try:
            output = subprocess.check_output(["powershell.exe", "Get-ComputerInfo"], shell=True)
            instance_type = re.search('CsName[ ]+: [a-z]+', output).group().split(':')[-1].strip()
            return instance_type
        except Exception as e:
            print('Error: {}'.format(e))
    else:
        return os.name


@announce
def matlabProcess(startpath='C:\Users\agridata\Projects\agridata\code'):
    '''
    Start MATLAB engine. This should not be global because it does not apply to all users of the script. Having said that,
    my hope is that it becomes a pain to pass around. The Windows path is a safe default that probably should be offloaded
    elsewhere since it
    '''
    logging.info('Starting MATLAB. . .')
    mlab = matlab.engine.start_matlab()
    mlab.addpath(mlab.genpath(startpath))

    return mlab


if __name__ == '__main__':
    ## Poller
    # poll()

    ## Manual scan
    #for scan in Scan.objects():
    #    try:
    #        initiateScanProcess(scan)
    #    except Exception as e:
    #        logging.info('A fnord error has occured: {}'.format(e))

    task = {
        'clientid'    = '5953469d1fb359d2a7a66287'    # Just a
        'scanid'      = '2017-06-27_23-37'            # test case
        'role'        = identifyRole(scan)
    }

    # Initial decision point. It is important that these do not return anything. This requires that each task stream
    # be responsible for handling its own end conditions, whether it be an graceful exit, an abrupt termination, etc. THe
    # reason is that that task itself has the sole responsibility of knowing what it should or should not be doing and how
    # to handle adverse or successful events.
    task = receivefromServiceBus(role)
    if role == 'worker-rvm':
        generateRVM(task)
    if role == 'worker-preprocess':
        preprocess(task)

