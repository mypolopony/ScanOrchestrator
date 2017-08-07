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
import pika
import boto3
import shutil
import socket
import traceback
import requests

import glob
import argparse
import multiprocess

import pandas as pd
import numpy as np

from bson.objectid import ObjectId
from botocore.exceptions import ClientError

# AgriData
from utils.models_min import *
from utils.connection import *

# Operational parameters
WAIT_TIME   = 20    # AWS wait time for messages
NUM_MSGS    = 10    # Number of messages to grab at a time
RETRY_DELAY = 60    # Number of seconds to wait upon encountering an error

# Load config file
config = ConfigParser.ConfigParser()
if os.name == 'nt':
    import matlab.engine
    config.read(r'C:\AgriData\Projects\ScanOrchestrator\utils\poller.conf')
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
service_bus = ServiceBusService(config.get('service_bus', 'service_namespace'),
                                config.get('service_bus', 'shared_access_key_name'),
                                config.get('service_bus', 'shared_access_key_value'))

# RabbitMQ connection (with generic channl)
rmq_connection = pika.BlockingConnection(pika.URLParameters('amqp://{}:{}@{}/{}'.format(
                                config.get('rmq', 'username'),
                                config.get('rmq', 'password'),
                                config.get('rmq', 'hostname'),
                                config.get('rmq', 'vhost'))))
channel = rmq_connection.channel()
# Ensure that queues exist
for q in ['rvm', 'preprocess', 'detection', 'process', 'postprocess', 'dlq']:
    channel.queue_declare(queue=q)

# Initialize logging
logger = logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]: %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S')
logger = logging.getLogger('default')
logger.setLevel(logging.DEBUG)

# File Handler
fh = logging.FileHandler('events.log')
fh.setLevel(logging.INFO)

# Console Handler
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# Formatter
formatter = logging.Formatter('%(asctime)s [%(levelname)s]: %(message)s', datefmt='%a, %d %b %Y %H:%M:%S')
ch.setFormatter(formatter)
fh.setFormatter(formatter)

# Add handlers
# logger.addHandler(ch)         # For sanity's sake, toggle console-handler and file-handler, but not both

# Canonical indows paths
video_dir = r'C:\AgriData\Projects\videos'

# Parameters                            # To calculate the number of separate tasks, 
SHARD_FACTOR = 4                        # divide the number of rows by this factor
NUM_MATLAB_INSTANCES = SHARD_FACTOR     # Each machine will spwn this number of MATLAB instances
RESULTS_PREFIX = 'azure'


def announce(func, *args, **kwargs):
    def wrapper(*args, **kwargs):
        logger.info('***** Executing {} *****'.format(func.func_name))
        return func(*args, **kwargs)

    return wrapper


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
def poll(args):
    start = datetime.datetime.now()

    logger.info('Scan Detector Starting\n\n')

    # Poll messages
    while True:
        try:
            logger.debug('Requesting tasks')
            try:
                results = queue.receive_messages(MaxNumberOfMessages=NUM_MSGS, WaitTimeSeconds=WAIT_TIME)
            except Exception as e:
                logger.error(traceback.print_exc())
                logger.error('A problem occured: {}'.format(str(e)))
                raise e

            # For each result
            logger.info('Received {} tasks'.format(len(results)))
            for ridx, result in enumerate(results):
                try:
                    logger.info('Queueing task {}/{}'.format(ridx, len(results)))
                    task = handleAWSMessage(result)             # Parse clientid and scanid
                    task['role'] = 'rvm'                        # Tag with the first task step
                    sendtoRMQ('rvm', task)    # Send to the RVM queue
                except:
                    logger.error(traceback.print_exc())
                    logger.exception('Error while handling messge: {}'.format(result.body))
                    # can add dead letter queue for poisonous messages here
            else:
                logger.debug('No tasks to do.')

        except Exception as e:
            # General errors, with a retry delay
            logger.error(traceback.print_exc())
            logger.exception('Unexpected error: {}. Sleeping for {} seconds'.format(str(e), RETRY_DELAY))

    logger.debug('Sleeping for {} seconds'.format(RETRY_DELAY))
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

    logger.info('Transforming Scan {} (client {})'.format(scan.scanid, scan.client))

    ## Rename tars and csvs in place on S3
    # This can be done without downloading any of the .tar.gz files, although they will
    # be downloaded later. This process is expected to take some time, as resource on S3
    # cannot be moved but must be copied.
    pattern_cam = re.compile('[0-9]{8}')
    for fidx, file in enumerate(s3.list_objects(Bucket=config.get('s3','bucket'),
                                                Prefix='{}/{}'.format(scan.client, scan.scanid))['Contents']):

        # Exclude the folder itself
        if file['Key'] != '{}/{}/'.format(str(scan.client), str(scan.scanid)) and not file['Key'].startswith(scan.scanid):
            # Extract camera and timestamp for rearrangement
            camera = re.search(pattern_cam, file['Key']).group()
            time = '_'.join([file['Key'].split('_')[-2], file['Key'].split('_')[-1]])

            # Tars
            if '.tar.gz' in file['Key']:
                newfile = '{}/{}/{}_{}_{}'.format(str(scan.client), str(scan.scanid), str(scan.scanid), camera, time)

            # CSVs
            if '.csv' in file['Key']:
                newfile = '{}/{}/{}_{}.csv'.format(str(scan.client), str(scan.scanid), str(scan.scanid), camera)

            # Copy (unless the file exists already). Boto3 will error when loading a non-existent object
            try:
                s3r.Object(config.get('s3', 'bucket'), newfile).load()
            except:
                pass

            # Copy (unless the file exists already)
            try:
                s3r.Object(config.get('s3', 'bucket'), newfile).load()
            except:
                logger.info('Renaming {} --> {}'.format(file['Key'], newfile))
                s3r.Object(config.get('s3', 'bucket'), newfile).copy_from(CopySource={'Bucket': config.get('s3', 'bucket'),
                                                                                      'Key': file['Key']})

            # Tthe Great Rename Fiasco of 2017 was brought about by this line. As files in the old format were deleted,
            # boxes in the field began to began to re-upload imagery, starting with the earliest scans. The line is kept
            # here as an historical exhibit.
            # s3r.Object(config.get('s3', 'bucket'), file['Key']).delete()

    # Download files required for tar inspection and cv update
    # Temporary file destination
    dest = os.path.join(tmpdir, str(scan.client), str(scan.scanid))
    if not os.path.exists(dest):
        os.makedirs(dest)

    # Rsync scan to temporary folder
    logger.info('Downloading scan files. . . this can take a while')
    
    # Download only the tars and csvs that have been renamed
    subprocess.call(['rclone',
                    '--include', '{}*'.format(scan.scanid),
                     '--config', '{}/.config/rclone/rclone.conf'.format(os.path.expanduser('~')),
                     'copy', '{}:{}/{}/{}'.format(config.get('rclone','profile'),
                                                   config.get('s3','bucket'),
                                                   str(scan.client),
                                                   str(scan.scanid)), dest])

    ## Add filenames column to CSV
    logger.info('Filling in CSV columns. . . this can also take a while. . .')
    offset = 0
    for csv in glob.glob(dest + '/{}*.csv'.format(scan.scanid)):
        camera  = re.search(pattern_cam, csv).group()
        log     = pd.read_csv(csv)
        archives = sorted(glob.glob(dest + '/*' + camera + '*.tar.gz'))
        try:
            for idx, tf in enumerate(archives):

                # Take the first frame number and remember it as the offset
                if idx == 0:
                    offset = log['frame_number'][0]
                    print('Camera {}, Offset {}'.format(camera, offset))
                print('{} : {}/{}'.format(camera, idx, len(archives)))
                try:
                    # These are the recalculated frame numbers, i.e. line numbers in the log
                    framenos = [int(m.name.replace('.jpg','')) - offset for m in tarfile.open(tf).getmembers()]
                    # Occasionally, there are more tar files than there are lines in the CSV, 
                    # but always no more than one (due to log file / image file race)
                    framenos = [f for f in framenos if f < len(log)]
                    log.loc[framenos,'filename'] = tf
                except:
                    logger.warning('Tar file {} could not be opened'.format(tf))
            log.to_csv(csv)
        except Exception as e:
            logger.error(traceback.print_exc())
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
    # recreate the base temporary directory.
    shutil.rmtree(dest)


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

    # IF there are no files that start with scanid, then we should convert!
    if not len([k for k in keys if k.split('/')[-1].startswith(scan.scanid)]):
        transformScan(scan)
    # print('This feature disabled; a more intelligent service is required to perform transformation')

    # The scan is uploaded and ready to be placed in the 'ready' or 'rvm' queue, where it
    # may be be evaluated for 'goodness'


@announce
def handleFailedTask(queue, task):
    '''
    Behavior for failed tasks
    '''
    MAX_RETRIES = 9         # Under peek.lock conditions, Azure sets a default of 10

    if 'num_retries' in task.keys() and task['num_retries'] >= MAX_RETRIES:
        log('Max retries met for task, sending to DLQ: {}'.format(task))
        sendtoRMQ('dlq', task)
    else:
        # Num_retries should exist, so this check is just for safety
        task['num_retries'] += 1
        log('Task FAILED. Re-enqueing: {}'.format(task))

        # Delete error message and reenqueue
        del task['message']
        sendtoRMQ(queue, task)


@announce
def sendtoServiceBus(service_bus, queue, msg):
    '''
    This is plainly just a wrapper for a message to a service bus, maybe other messaging things will happen here
    '''
    msg['num_retries'] = 0              # Reset number of retries
    service_bus.send_queue_message(queue, Message(json.dumps(msg)))


@announce
def receivefromServiceBus(service_bus, queue, lock=False):
    '''
    A generic way of asking for work to do, based on the task. The default behavior, for longer running processes,
    is for peek_lock to be False, which means deleting the message when it is received. This means that the
    task process is responsible for re-enqueing the task if it fails. This 
    '''
    incoming = None
    while not incoming:
        incoming = service_bus.receive_queue_message(queue, peek_lock=lock).body

    # Here, we can accept either properly formatted JSON strings or, if necessary, convert the
    # quotes to make them compliant.
    try:
        msg = json.loads(incoming)
    except:
        msg = json.loads(incoming.replace("'",'"').replace('u"','"'))

    return msg


@announce
def sendtoRMQ(queue, message):
    if type(message) is not dict:
        message = json.dumps(message)

    channel.basic_publish(exchange='',
                          routing_key=queue,
                          body=json.dumps(message))


@announce
def emitSNSMessage(message, context=None, topic='statuslog'):
    # A simple notification payload, sent to all of a topic's subscribed endpoints

    # Ensure type (change to JSON)
    if type(message) == str:
        _message = dict()				# Save as temp variable
        _message = {'info': message}
        message = _message				# Unwrap

    # Main payload
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
def log(message):
    # Let's send this message to the dashboard 
    payload = dict()
    payload['hostname'] = socket.gethostname()
    payload['ip'] = socket.gethostbyname(payload['hostname'])
    payload['message'] = message

    try:
        r = requests.post('http://dash.agridata.ai/orchestrator', json = payload)
    except Exception as e:
        logger.warning('Boringmachine not reachable: {}'.format(e))


@announce
def generateRVM(ch, method, properties, body):
    '''
    Given a set of scans (or one scan), generate a row video map.
    '''
    # The task
    task = json.loads(body)

    # Notify
    log('Received task: {}'.format(task))

    # Start MATLAB
    mlab = matlabProcess()

    try:
        # Obtain the scan
        try:
            scan = Scan.objects.get(client=task['clientid'], scanid=task['scanids'][0])
        except:
            scan = Scan.objects.get(client=ObjectId(task['clientid']), scanid=task['scanids'][0])

        block = Block.objects.get(id=scan.blocks[0])
        task['blockid']     = str(block.id)
        task['blockname']   = block.name
        task['farmid']      = str(block.farm)
        task['farmname']    = Farm.objects.get(id=block.farm).name.replace(' ','')

        # Check staging database for previous scans of this block
        # staged = db.staging.find({'block': scan.blocks[0]})
        # if staged:
        #    task['scanids'] = task['scanids'] + [s['_id'] for s in staged]

        # Send the arguments off to batch_auto, return is the S3 location of rvm.csvs
        log('Calculating RVM')
        s3uri, localuri = mlab.runTask('rvm', task['clientid'], task['scanids'], task)
        task['rvm_uri'] = s3uri

        # Check for completeness
        data = pd.read_csv(localuri, header=0)
        rows_found = len(set([(r, d) for r,d in zip(data['rows'],data['direction'])])) / 2
        if rows_found < block.num_rows * 0.5:
            emitSNSMessage('RVM is not long enough (found {}, expected {})! [{}]'.format(rows_found, block.num_rows, task))
        elif rows_found > block.num_rows:
            emitSNSMessage('Too many rows found (found {}, expected {})! [{}]'.format(rows_found, block.num_rows, task))
        else:
            # Generate tar files and eliminate NaNs
            tarfiles = pd.Series.unique(data['file'])

            # Split tarfiles
            for shard in np.array_split(tarfiles, int(len(tarfiles)/SHARD_FACTOR)):
                task['tarfiles'] = [s for s in shard if s]
                task['num_retries'] = 0         # Set as clean
                sendtoRMQ('preprocess', task)

            log('RVM task complete')
    except Exception as err:
        emitSNSMessage('Failure on {}'.format(str(err)))
        pass


@announce
def rebuildScanInfo(task):
    if os.path.exists(video_dir):
        shutil.rmtree(video_dir)
    os.makedirs(video_dir)
    os.makedirs(video_dir + '\imu_basler')

    # Download log files
    for scan in task['scanids']:
        for file in s3.list_objects(Bucket=config.get('s3','bucket'),Prefix='{}/{}'.format(task['clientid'], scan))['Contents']:
            key = file['Key'].split('/')[-1]
            if 'csv' in key and key.startswith(scan):
                s3r.Bucket(config.get('s3', 'bucket')).download_file(file['Key'], os.path.join(video_dir, 'imu_basler', key))

    # Download the RVM, VPR
    key = '/'.join(task['rvm_uri'].split('/')[3:])
    s3r.Bucket(config.get('s3', 'bucket')).download_file(key, os.path.join(video_dir, key.split('/')[-1]))
    key = key.replace('rvm.csv','vpr.csv')
    s3r.Bucket(config.get('s3', 'bucket')).download_file(key, os.path.join(video_dir, key.split('/')[-1]))


@announce
def preprocess(ch, method, properties, body):
    '''
    Preprocessing method
    '''
    # The task
    task = json.loads(body)

    # Notify
    log('Received task: {}'.format(task))

    # Start MATLAB
    mlab = matlabProcess()

    try:
        # Rebuild base scan info
        rebuildScanInfo(task)

        # Download the tarfiles
        for tar in task['tarfiles']:
            scanid = '_'.join(tar.split('_')[0:2])
            key = '{}/{}/{}'.format(task['clientid'], scanid, tar)
            log('Downloading {}'.format(key))
            s3r.Bucket(config.get('s3','bucket')).download_file(key, os.path.join(video_dir, tar))

        # Untar
        mlab = matlabProcess()
        mlab.my_untar(video_dir, nargout=0)
        mlab.quit()

        # These are the processes to be spawned. They call to the launchMatlabTasks wrapper primarily
        # because the multiprocessing library could not directly be called as some of the objects were
        # not pickleable? The multiprocess library (notice the spelling) overcomes this, so I don't think
        # the function wrapper is necessary anymore
        log('Preprocessing {} archives with {} MATLAB instances'.format(len(task['tarfiles']), NUM_MATLAB_INSTANCES))
        workers = list()
        for instance in range(NUM_MATLAB_INSTANCES):
            worker = multiprocess.Process(target=launchMatlabTasks, args=['preprocess', task])
            worker.start()
            workers.append(worker)

            # Short delay to stagger workers
            time.sleep(2)

        for worker in workers:
            worker.join()

        log('All MATLAB instances have finished')

        # Pre file upload, recreate relevant parts of analysis_struct
        analysis_struct = dict.fromkeys(['video_folder', 's3_result_path'])
        analysis_struct['video_folder'] = video_dir

        # S3 results path
        analysis_struct['s3_result_path'] = 's3://agridatadepot.s3.amazonaws.com/{}/results/farm_{}/block_{}'.format(task['clientid'], task['farmname'].replace(' ',''), task['blockname'].replace(' ',''))

        # Hand-off to detection
        zips = glob.glob(analysis_struct['video_folder'] + '/*.zip')
        log('Success, found {} zip files. Creating Detection tasks.'.format(len(zips)))
        for zipfile in zips:
            detectiontask = task
            detectiontask['detection_params'] =  dict(
                bucket='agridatadepot',
                base_url_path='{}/results/farm_{}/block_{}/{}'.format(task['clientid'],task['farmname'].replace(' ',''), task['blockname'].replace(' ', ''),RESULTS_PREFIX),
                input_path='preprocess-frames',
                output_path='detection',
                caffemodel_s3_url_cluster='s3://deeplearning_data/models/best/post-bloom_july_29_2017_390000.caffemodel',
                caffemodel_s3_url_trunk='s3://deeplearning_data/models/best/trunk_june_10_400000.caffemodel',
                s3_aws_access_key_id='AKIAJC7XVEAQELBKAANQ',
                s3_aws_secret_access_key='YlPBiE9s9LV5+ruhKqQ0wsZgj3ZFp6psF6p5OBpZ',
                session_name= datetime.datetime.now().strftime('%m-%d-%H-%M-%S.%f').replace('.','-'),
                folders=[ os.path.basename(zipfile) ])
            detectiontask['num_retries'] = 0         # Set as clean
            sendtoRMQ('detection', detectiontask)
    except ClientError:
        # For some reason, 404 errors occur all the time -- why? Let's just ignore them for now and replace the queue in the task
        sendtoRMQ('preprocess', task)
        pass
    except Exception as e:
        task['message'] = traceback.print_exc() + ' : ' + e
        handleFailedTask('preprocess', task)
        pass


@announce 
def launchMatlabTasks(taskname, task):
    '''
    A separate wrapper for multiple matlabs. It is called by multiprocess, which has the capability to
    pickle (actually, dill) a wider range of objects (like function wrappers)
    '''
    mlab = matlabProcess()
    mlab.runTask(taskname, task['clientid'], task['scanids'], task)
    mlab.quit()


@announce
def detection(args):
    '''
    Detection method
    '''
    print('Koshyland')


@announce
def process(ch, method, properties, body):
    '''
    Processing method
    '''
     # The task
    task = json.loads(body)

    # Notify
    log('Received task: {}'.format(task))

    # Start MATLAB
    mlab = matlabProcess()

    try:
        # Rebuild base scan info
        rebuildScanInfo(task)

        # Wait for a group of scans equal to the number of MATLAB instances
        while len(multi_task) < NUM_MATLAB_INSTANCES and service_bus.get_queue('process').message_count > 0:
            multi_task.append(receivefromServiceBus(args.service_bus, 'process'))

        # Launch tasks
        log('Processing group of {} archives'.format(len(multi_task)))
        workers = list()
        for task in multi_task:
            for zipfile in task['detection_params']['folders']:
                scanid = '_'.join(zipfile.split('_')[0:2])
                key = '{}/results/farm_{}/block_{}/{}/detection/{}'.format(task['clientid'], task['farmname'].replace(' ',''), task['blockname'].replace(' ',''), RESULTS_PREFIX, zipfile)
                log('Downloading {}'.format(key))
                s3r.Bucket(config.get('s3','bucket')).download_file(key, os.path.join(video_dir, zipfile))
            worker = multiprocess.Process(target=launchMatlabTasks, args=['process', task])
            worker.start()
            workers.append(worker)

            # Delay is recommended to keep MATLAB processes from stepping on one another
            time.sleep(4)

        for worker in workers:
            worker.join()

        log('All MATLAB instances have finished')

        # Pre file upload, recreate relevant parts of analysis_struct
        analysis_struct = dict.fromkeys(['video_folder', 's3_result_path'])
        analysis_struct['video_folder'] = video_dir

        # S3 results path
        analysis_struct['s3_result_path'] = 's3://agridatadepot.s3.amazonaws.com/{}/results/farm_{}/block_{}'.format(task['clientid'], task['farmname'].replace(' ',''), task['blockname'].replace(' ',''))

        # Now what?
        log('Processing done. {}'.format(task))
    except ClientError:
        # For some reason, 404 errors occur all the time -- why? Let's just ignore them for now and replace the queue in the task
        sendtoRMQ('process', task)
        pass
    except Exception as e:
        task['message'] = e
        handleFailedTask('process', task)
        pass


@announce
def identifyRole():
    '''
    This wmethod is called when an instance starts. Based on its ComputerInfo, it will identify itself as a
    member of a particular work group and then proceeded to execute the correct tasks.
    '''

    # Windows box
    if os.name == 'nt':
        try:
            # Look for computer type (role)
            output = subprocess.check_output(["powershell.exe", "Get-ComputerInfo"], shell=True)
            instance_type = re.search('CsName[ ]+: \w+', output).group().split(':')[-1].strip().lower()
            return instance_type
        except Exception as e:
            logger.error(traceback.print_exc())
            logging.error('Error: {}'.format(e))

    # Linux box
    else:
        return os.name


@announce
def matlabProcess(startpath=r'C:\AgriData\Projects'):
    '''
    Start MATLAB engine. This should not be global because it does not apply to all users of the script. Having said that,
    my hope is that it becomes a pain to pass around. The Windows path is a safe default that probably should be offloaded
    elsewhere since it
    '''
    logger.info('Starting MATLAB. . .')
    mlab = matlab.engine.start_matlab()
    mlab.addpath(mlab.genpath(startpath))

    return mlab


@announce
def getComputerInfoString():
	# Grab
	ret = subprocess.check_output('ipconfig')

	# FOrmat
	ignore = [' . ','\r','\n']
	for i in ignore:
		ret = ret.replace(i,'')

	return ret


def parse_args():
    parser=argparse.ArgumentParser('orchestrator')

    default_service_namespace = 'agridataqueues'
    default_shared_access_key_name = 'sharedaccess'
    default_shared_access_key_value = 'cWonhEE3LIQ2cqf49mAL2uIZPV/Ig85YnyBtdb1z+xo='
    parser.add_argument('-n', '--service_namespace', help='service namespace', dest='service_namespace',
                        default=default_service_namespace)
    parser.add_argument('-k', '--shared_access_key_name', help='shared_access_key_name', dest='shared_access_key_name',
                        default=default_shared_access_key_name)
    parser.add_argument('-v', '--shared_access_key_value', help='shared_access_key_value', dest='shared_access_key_value',
                        default=default_shared_access_key_value)
    args = parser.parse_args()
    args.service_bus = ServiceBusService(service_namespace=args.service_namespace,
                                shared_access_key_name=args.shared_access_key_name,
                                shared_access_key_value=args.shared_access_key_value)
    return args


if __name__ == '__main__':
    # Initial decision points. It is important that these do not return anything. This requires that each task stream
    # be responsible for handling its own end conditions, whether it be an graceful exit, an abrupt termination, etc. The
    # reason is that that task itself has the sole responsibility of knowing what it should or should not be doing and how
    # to handle adverse or successful events.

    # What task are we meant to do? This is based on instance names

    args = parse_args()
    roletype = identifyRole()
    log('I\'m awake! Role type is {}'.format(roletype))

    try:
        # RVM Generation
        elif 'rvm' in roletype or 'jumpbox' in roletype:
            channel.basic_consume(generateRVM, queue='rvm', no_ack=True)

        # Preprocessing
        if 'preproc' in roletype:
            channel.basic_consume(preprocess, queue='preprocess', no_ack=True)

        # Detection
        elif 'detection' in roletype::
            channel.basic_consume(detection, queue='detection', no_ack=True)

        # Process
        elif 'process' in roletype or 'xob' in roletype:
            channel.basic_consume(process, queue='process', no_ack=True)

        # Convert scan filenames and CSVs from old style to new style
        elif roletype == 'convert':
            scans = Scan.objects()

            for scan in scans:
                try:
                    initiateScanProcess(scan)
                except Exception as e:
                    logger.error(traceback.print_exc())
                    logger.info('An error has occured: {}'.format(e))

        # AWS daemon mode
        elif roletype == 'poll':
            poll(args)

        # Error
        else:
            emitSNSMessage('Could not determine role type.\n{}'.format(getComputerInfoString))
    except Exception as e:
        # TODO: Capture other signals to close the RMQ connection?
        emitSNSMessage(str(e))
        rmq_connection.close()