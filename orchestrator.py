#!/usr/bin/python
import os
import sys
import ConfigParser
import argparse
import glob
import json
import logging
import re
import shutil
import socket
import subprocess
import tarfile
import time
import traceback
import psutil

import boto3
import multiprocess
import numpy as np
import pandas as pd
import requests
from botocore.exceptions import ClientError
from bson.objectid import ObjectId
from kombu import Connection, Consumer, Producer, Exchange, Queue
from kombu.utils.compat import nested


#some relative paths needed for detection in linux
#it needto access files in ../deepLearning module
if os.name == 'posix':
    from inspect import getsourcefile
    current_path = os.path.abspath(getsourcefile(lambda:0))
    parent_dir = os.path.split(os.path.dirname(current_path))[0]
    sys.path.insert(0, parent_dir)
    source_dir=os.path.dirname(current_path)

# AgriData Database Connection
from utils.connection import *

# Operational parameters (for AWS)
WAIT_TIME = 20      # [AWS] Wait time for messages
NUM_MSGS = 10       # [AWS] Number of messages to grab at a time
RETRY_DELAY = 60    # [AWS] Number of seconds to wait upon encountering an error
CORES = 4           # [GENERAL] Number of cores (= number of MATLAB instances)

# OS-Specific Setup
if os.name == 'nt':
    # Canonical windows paths
    base_windows_path = r'C:\AgriData\Projects\\'

    config_dir = r'C:\AgriData\Projects\ScanOrchestrator'
    import matlab.engine
else:
    assert (os.path.basename(os.getcwd()) == 'ScanOrchestrator')
    config_dir = '.'

# Load config file
config = ConfigParser.ConfigParser()
config_path = os.path.join(config_dir, 'utils', 'poller.conf')
config.read(config_path)

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
sns = boto3.client('sns', region_name=config.get('sns', 'region'))

# Kombu connection (persistent)
rabbit_url = '{}:{}@{}'.format(config.get('rmq', 'username'), config.get('rmq', 'password'), config.get('rmq', 'hostname'))
conn = Connection('amqp://{}:5672//'.format(rabbit_url)).connect()

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
logger.addHandler(ch)           # For sanity's sake, toggle console-handler and file-handler, but not both
logger.addHandler(fh)

# Miscellany
task, multi_task = None, None   # Avoid annoying failure messages if these are not defined


def announce(func, *args, **kwargs):
    '''
    Default decorator to send the current operation to the log
    '''
    def wrapper(*args, **kwargs):
        logger.info('***** Executing {} *****'.format(func.func_name))
        return func(*args, **kwargs)

    return wrapper


def establish_connection():
    revived_connection = Connection('amqp://{}:5672//'.format(rabbit_url)).connect()
    revived_connection.ensure_connection(max_retries=3)
    channel = fresh_connection.channel()
    consumer.revive(channel)
    consumer.consume()
    print('Fresh connection!')
    return fresh_connection


def process_message(body, message):
    print('Message received: {}\n\t{}'.format(message,body))
    message.ack()


def list_bound_queues(exchange):
    '''
    In a canonical AMQP framework, there is no way of listing an Exchange's bindings. Canonically,
    publishers publish messages to an exchange and consumers declare queues and determine routing.

    Since a machine will not have access to routing keys (session_names), we must use the Management
    Plugin and call for a list of the bindings using the HTTP API
    '''
    r = requests.get('http://{}:15672/api/bindings/%2f/'.format(rabbit_url))
    bindings = [binding for binding in r.json() if binding['source'] == exchange]
    return [Queue(b['destination']) for b in bindings]


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
    task['clientid'] = obj['key'].split('/')[0]
    task['scanid'] = obj['key'].split('/')[1]
    task['filename'] = obj['key'].split('/')[2]
    task['size'] = float(obj['size']) / 1024.0

    logger.info('\tWorking on {} ({} MB)'.format(task['filename'], task['size']))

    for goodkey in ['clientid', 'scanid', 'filename', 'size']:
        logger.info('\t{}\t{}'.format(goodkey.capitalize(), task[goodkey]))

        # Delete task / Re-enque task on fail
        # When tasks are received, they are temporarily marked as 'taken' but it is up to this process to actually
        # delete them or, failing that, default to releasing them back to the queue

@announce
def poll():
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
                raise Exception(e)

            # For each result
            logger.info('Received {} tasks'.format(len(results)))
            for ridx, result in enumerate(results):
                try:
                    logger.info('Queueing task {}/{}'.format(ridx, len(results)))
                    task = handleAWSMessage(result)  # Parse clientid and scanid
                    task['role'] = 'rvm'  # Tag with the first task step
                    sendtoRabbitMQ(args,'rvm', task)  # Send to the RVM queue
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
def log(message, session_name=''):
    '''
    Let's send this message to the dashboard
    '''
    # TODO: Instead of sending session, we should send the task itself
    payload = dict()
    payload['hostname'] = socket.gethostname()
    payload['ip'] = socket.gethostbyname(payload['hostname'])
    payload['message'] = message
    payload['session_name'] = session_name

    try:
        r = requests.post('http://dash.agridata.ai/orchestrator', json=payload)
    except Exception as e:
        logger.warning('Boringmachine not reachable: {}'.format(e))


@announce
def sendtoRabbitMQ(package):
    chan = conn.channel()
    q = Queue('_'.join([package['role'], package['session_name']]), exchange=Exchange(task['role']), routing_key=package['session_name'], channel=chan)
    q.put(package)
    q.close()


@announce
def handleFailedTask(task):
    '''
    Behavior for failed tasks
    '''
    MAX_RETRIES = 9  # Under peek.lock conditions, Azure sets a default of 10, so let's catch before that

    if 'num_retries' in task.keys() and task['num_retries'] >= MAX_RETRIES:
        log('Max retries met for task, sending to DLQ: {}'.format(task))
        task['role'] = 'dlq'
        sendtoRabbitMQ(task)
    else:
        # Num_retries should exist, so this check is just for safety
        task['num_retries'] += 1
        log('Task FAILED. Re-enqueing: {}'.format(task), task['session_name'])

        # Delete error message and reenqueue
        del task['message']
        sendtoRabbitMQ(task)


@announce
def emitSNSMessage(message, context=None, topic='statuslog'):
    # A simple notification payload, sent to all of a topic's subscribed endpoints

    # Ensure type (change to JSON)
    if type(message) == str:
        _message = dict()  # Save as temp variable
        _message = {'info': message}
        message = _message  # Unwrap

    # Main payload
    payload = {
        'message': message,
        'topic_arn': aws_arns[topic],
        'context': context,
        'hostname': socket.gethostname(),
        'ip': socket.gethostbyname(socket.gethostname())
    }

    # Emit the message
    response = sns.publish(
        TopicArn=aws_arns[topic],
        Message=json.dumps(payload),  # SNS likes strings
        Subject='{} message'.format(topic)
    )


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
    for fidx, file in enumerate(s3.list_objects(Bucket=config.get('s3', 'bucket'),
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
                s3r.Object(config.get('s3', 'bucket'), newfile).copy_from(
                    CopySource={'Bucket': config.get('s3', 'bucket'),
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
                     'copy', '{}:{}/{}/{}'.format(config.get('rclone', 'profile'),
                                                  config.get('s3', 'bucket'),
                                                  str(scan.client),
                                                  str(scan.scanid)), dest])

    ## Add filenames column to CSV
    logger.info('Filling in CSV columns. . . this can also take a while. . .')
    offset = 0
    for csv in glob.glob(dest + '/{}*.csv'.format(scan.scanid)):
        camera = re.search(pattern_cam, csv).group()
        log = pd.read_csv(csv)
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
                    framenos = [int(m.name.replace('.jpg', '')) - offset for m in tarfile.open(tf).getmembers()]

                    # Occasionally, there are more tar files than there are lines in the CSV,
                    # but always no more than one (due to log file / image file race)
                    framenos = [f for f in framenos if f < len(log)]

                    # Use basename here to avoid full paths in frame log / RVM
                    log.loc[framenos, 'filename'] = os.path.basename(tf)
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
def convert(scan):
    '''
    This is a bouncer for incoming scans before they are transformed. Basically, it only transform the scan
    if it needs to be transformed
    '''

    ## Check for formatting (old style -> new style)
    # These are the keys (files) in the scan directory
    s3base = '{}/{}'.format(str(scan.client), str(scan.scanid))
    keys = [obj['Key'] for obj in s3.list_objects(Bucket=config.get('s3', 'bucket'), Prefix=s3base)['Contents']]

    # If there are no files that start with scanid, then we should convert!
    if not len([k for k in keys if k.split('/')[-1].startswith(scan.scanid)]):
        transformScan(scan)


@announce
def generateRVM(task, message):
    '''
    Generate a row video map.
    '''
    try:
        message.ack()
        log('Received task: {}'.format(task), task['session_name'])

        rebuildScanInfo(task)

        # Start MATLAB
        mlab = matlabProcess()

        log('Calculating RVM', task['session_name'])
        mlab.runTask(task, nargout=0)

        # Check for completeness
        local_uri = os.path.join(base_windows_path, task['session_name'], 'videos', 'rvm.csv')
        data = pd.read_csv(local_uri, header=0)
        rows_found = len(set([(r, d) for r, d in zip(data['rows'], data['direction'])])) / 2
        block = Block.objects.get(id=task['blockid'])
        if rows_found < block.num_rows * 0.5:
            emitSNSMessage(
                'RVM is not long enough (found {}, expected {})! [{}]'.format(rows_found, block.num_rows, task))
        elif rows_found > block.num_rows:
            emitSNSMessage(
                'Too many rows found (found {}, expected {})! [{}]'.format(rows_found, block.num_rows, task))
        else:
            # Generate tar files and eliminate NaNs
            tarfiles = pd.Series.unique(data['file'])

            # Split tarfiles
            for tf in tarfiles:
                task['tarfiles'] = [tf]
                task['num_retries'] = 0                     # Initialize as clean
                task['role'] = 'preprocess'                 # Next step
                sendtoRabbitMQ(task)

            log('RVM task complete', task['session_name'])
    except Exception as err:
        emitSNSMessage('Failure on {}'.format(str(err)), context=traceback.format_exc())
        pass


@announce
def rebuildScanInfo(task):
    # Robust directory creation here
    success = False

    session_dir = os.path.join(base_windows_path, task['session_name'])
    while not success:
        try:
            if os.path.exists(session_dir):
                shutil.rmtree(session_dir)

            # Wait for a second: the previous call can sometimes returns early
            time.sleep(5)
            os.makedirs(os.path.join(base_windows_path, task['session_name'], 'videos', 'imu_basler'))
            os.makedirs(os.path.join(base_windows_path, task['session_name'], 'results'))
            os.makedirs(os.path.join(base_windows_path, task['session_name'], 'code'))
            os.makedirs(os.path.join(base_windows_path, task['session_name'], 'csv'))
            success = True
        except IOError as e:
            log('Rebuilding IO Error: {}'.format(e), task['session_name'])
            raise Exception(e)
            pass
        except Exception as e:
            log('A serious error has occurred rebuilding scan info: {}'.format(e), task['session_name'])
            raise Exception(e)

    # Download log files
    for scan in task['scanids']:
        for file in s3.list_objects(Bucket=config.get('s3', 'bucket'), Prefix='{}/{}'.format(task['clientid'], scan))['Contents']:
            key = file['Key'].split('/')[-1]
            if 'csv' in key and key.startswith(scan):
                s3r.Bucket(config.get('s3', 'bucket')).download_file(file['Key'],
                                                                     os.path.join(base_windows_path,
                                                                     task['session_name'], 'videos', 'imu_basler', key))

    # Download the RVM, VPR
    if task['role'] != 'rvm':
        s3_result_path = '{}/results/farm_{}/block_{}/{}/'.format(task['clientid'], task['farmname'].replace(' ',''), task['blockname'].replace(' ',''), task['session_name'])
        for csvfile in ['rvm.csv', 'vpr.csv']:
            s3r.Bucket(config.get('s3', 'bucket')).download_file(s3_result_path + csvfile, os.path.join(base_windows_path,
                                                                         task['session_name'], 'videos', csvfile))


@announce
def monitorMatlabs(workers, session_name):
    # Monitor the number of MATLAB processes
    matlabs = NUM_MATLAB_INSTANCES
    while matlabs > 0:
        log('MATLAB instances still alive: {}. Waiting for two minutes.'.format(matlabs), session_name)
        time.sleep(120)
        matlabs = len([p.pid for p in psutil.process_iter() if p.name().lower() == 'matlab.exe'])

    # All of the MATLABs are done, let's kill any lingering workers
    for worker in workers:
        try:
            worker.terminate()
        except:
            log('Terminate failed but that''s okay', session_name)


@announce
def preprocess(args):
    '''
    Preprocessing method
    '''
    while True:
        try:
            # The task
            task = receivefromRabbitMQ(args, 'preprocess')

            # Notify
            log('Received task: {}'.format(task), task['session_name'])

            # Rebuild base scan info
            rebuildScanInfo(task)

            # Download the tarfiles
            for tar in task['tarfiles']:
                scanid = '_'.join(tar.split('_')[0:2])
                key = '{}/{}/{}'.format(task['clientid'], scanid, tar)
                log('Downloading {}'.format(key), task['session_name'])
                s3r.Bucket(config.get('s3', 'bucket')).download_file(key, os.path.join(video_dir, tar))

            # Untar
            mlab = matlabProcess()
            mlab.my_untar(video_dir, task['session_name'], nargout=0)
            mlab.quit()

            # Generate subtasks. We are going to give each MATLAB instance a particular subset of
            # files
            subtars = np.array_split(task['tarfiles'], NUM_MATLAB_INSTANCES)

            # These are the processes to be spawned. They call to the launchMatlabTasks wrapper primarily
            # because the multiprocessing library could not directly be called as some of the objects were
            # not pickleable? The multiprocess library (notice the spelling) overcomes this, so I don't think
            # the function wrapper is necessary anymore
            log('Preprocessing {} archives with {} MATLAB instances'.format(len(task['tarfiles']),
                                                                            NUM_MATLAB_INSTANCES), task['session_name'])
            workers = list()
            for instance in range(NUM_MATLAB_INSTANCES):
                # Pick up subtasks
                subtask = task
                subtask['tarfiles'] = list(subtars[instance])

                # Run
                worker = multiprocess.Process(target=launchMatlabTasks, args=[args, subtask])
                worker.start()
                workers.append(worker)

                # Short delay to stagger workers
                time.sleep(5)

            # Blocking call to wait for workers to finish. MATLABs will send to detection
            monitorMatlabs(workers, task['session_name'])

        except ClientError:
            # For some reason, 404 errors occur all the time -- why? Let's just ignore them for now and replace the queue in the task
            sendtoRabbitMQ(args,'preprocess', task)
            pass
        except Exception as e:
            task['message'] = e
            handleFailedTask(args, 'preprocess', task)
            pass


@announce
def launchMatlabTasks(args, task):
    '''
    A separate wrapper for multiple matlabs. It is called by multiprocess, which has the capability to
    pickle (actually, dill) a wider range of objects (like function wrappers)
    '''
    try:
        mlab = matlabProcess()
        mlab.runTask(task, nargout=0)
        mlab.quit()
    except Exception as e:
        task['message'] = e
        handleFailedTask(args, task['role'], task)
        pass


@announce
def detection(args):
    '''
    Detection method
    '''
    from deepLearning.infra import detect_s3_az
    # logger.info('Config read: type:{}'.format(dict(config['env'])))
    while True:
        try:
            log('Waiting for task')
            task = receivefromRabbitMQ(args, 'detection')
            if type(task) != dict:
                task = json.loads(task)
            log('Received detection task: {}'.format(task), task['session_name'])

            arg_list = []

            # The task may come in unicode -- let's change to regular string
            for k, v in task['detection_params'].iteritems():
                arg_list.append('--' + k)
                if type(v) == list:
                    for vi in v:
                        arg_list.append(str(vi))
                else:
                    arg_list.append(str(v))


            args_detect = detect_s3_az.parse_args(arg_list)
            print(args_detect)
            logger.info(('detection process %r, %r' % (task, args_detect)))
            s3keys = detect_s3_az.main(args_detect)

            # for now we expect only one element
            assert (len(s3keys) <= 1)
            task['detection_params']['result'] = s3keys if not s3keys  else  s3keys[0]
            log('Success. Completed detection: {}'.format(task['detection_params']['result']), task['session_name'])
            sendtoRabbitMQ(args,'process', task)
        except Exception, e:
            tb = traceback.format_exc()
            logger.error(tb)
            task['message'] = str(e)
            log('args, Task FAILED. Re-enqueueing... ({})'.format(task), task['session_name'])
            handleFailedTask(args, 'detection', task)
            pass


@announce
def process():
    '''
    Processing method
    '''
    while True:
        try:
            # The task
            multi_task = receivefromRabbitMQ(args, 'process', num=NUM_MATLAB_INSTANCES)
            session_name = multi_task[0]['session_name']

            # Change roletype and reformat message
            # TODO: Does this really require using idx / enumerate? For some reason, multi_task is not cooperating otherwise
            for idx,m in enumerate(multi_task):
                if type(m) == unicode:
                    # MATLAB seems to prefer to send strings with single quotes, which needs to be converted
                    multi_task[idx] = json.loads(m.replace("u'", "'").replace("'", '"'))
                # The detection params reult is sometimes a string and sometimes a list -- is this because of the above?
                if type(multi_task[idx]['detection_params']['result']) == unicode:
                    multi_task[idx]['detection_params']['result'] = [multi_task[idx]['detection_params']['result']]

                multi_task[idx]['role'] = 'process'

            # Notify
            log('Received processing tasks: {}'.format([multi_task[0]['detection_params']['result'] for m in multi_task]), multi_task[0]['session_name'])

            # Rebuild base scan info (just use the first task)
            rebuildScanInfo(multi_task[0])

            # Launch tasks
            log('Processing group of {} archives'.format(len(multi_task)), session_name)
            workers = list()
            for task in multi_task:
                for zipfile in task['detection_params']['result']:
                    log('Downloading {}'.format(zipfile), task['session_name'])
                    s3r.Bucket(config.get('s3', 'bucket')).download_file(zipfile, os.path.join(video_dir, os.path.basename(zipfile)))
                worker = multiprocess.Process(target=launchMatlabTasks, args=[args, task])
                worker.start()
                workers.append(worker)

                # Delay is recommended to keep MATLAB processes from stepping on one another
                time.sleep(4)

            # Blocking call to wait for workers to finish. MATLABs will send to postprocess
            monitorMatlabs(workers,session_name)

        except ClientError:
            # For some reason, 404 errors occur all the time -- why? Let's just ignore them for now and replace the queue in the task
            sendtoRabbitMQ(args,'dlq', multi_task)
            pass
        except Exception as e:
            log('FAILED, SENDING TO DLQ: {}'.format(str(e)), multi_task[0]['session_name'])
            log(logger.error(traceback.print_exc()))
            sendtoRabbitMQ(args, 'dlq', multi_task)
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

    # Format
    ignore = [' . ', '\r', '\n']
    for i in ignore:
        ret = ret.replace(i, '')

    return ret


def windows_client():
    '''
    Since Windows machines can perform either preprocessing / processing equally, one strategy is to have
    any
    '''

    # Define consumers
    rvm_channel = conn.channel()
    preprocess_channel = conn.channel()
    process_channel = conn.channel()

    while True:
        try:
            with nested(Consumer(rvm_channel, list_bound_queues(exchange='rvm'), callbacks=[generateRVM], auto_declare=False),
                        Consumer(preprocess_channel, list_bound_queues(exchange='preprocess'), callbacks=[preprocess], auto_declare=False),
                        Consumer(process_channel,list_bound_queues(exchange='process'), callbacks=[process], auto_declare=False)):
                conn.drain_events(timeout=2)
        except socket.timeout:
            pass
        except conn.connection_errors:
            emitSNSMessage('Connection has been lost')
            break


def linux_client():
    '''
    For the detection machines
    '''
    detection_channel = conn.channel()
    consumer = Consumer(channel=chan, queues=list_bound_queues('detection'), callbacks=[detection], auto_declare=False)
    consumer.consume()

    while True:
        try:
            conn.drain_events(timeout=2)
        except socket.timeout:
            pass
        except conn.connection_errors:
            cmitSNSMessage('Connection has been lost')
            break



def run(role=None):
    '''
    The main entry point, which can either be called via import or executed from the command line
    '''
    try:
        role = role or os.name

        log('I\'m awake! Role type is {}'.format(role))

        # Specialty roles up front
        # Scan filename / log file conversion
        if role == 'convert':
            for scan in Scan.objects():
                convert(scan)

         # AWS poller (for new uploads)
        elif role == 'poll':
            poll()

        # RVM / Preprocessing / Processing
        elif role in ['nt', 'rvm', 'preprocess', 'process']:
            windows_client()

        # Detection
        elif ['posix', 'detection']:
            linux_client()

        # Unknown
        else:
            emitSNSMessage('Could not determine role type.\n{}'.format(getComputerInfoString))

    # Overarching error
    except Exception as e:
        emitSNSMessage(str(e), context=traceback.format_exc())


def parse_args():
    '''
    Add other parameters here
    '''
    parser=argparse.ArgumentParser('orchestrator')
    parser.add_argument('-r', '--role', help='role', dest='role', default=None)

    args = parser.parse_args()
    return args


if __name__ == '__main__':
    '''
    This is the main control entrypoint if the script is launched from the command line. Parameters can be
    addeed here, but it is kept simple for now
    '''
    # Arguments
    # args = parse_args()
    run()
