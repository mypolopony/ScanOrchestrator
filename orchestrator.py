#!/usr/bin/python
import os
import sys
import ConfigParser
import copy
import argparse
import glob
import json
import random
import logging
import re
import shutil
import socket
import subprocess
import multiprocess
import tarfile
import time
import traceback
import pandas as pd
import numpy as np
import requests

from datetime import datetime
from utils import RedisManager
from io import BytesIO, StringIO
from utils.models_min import dotdict, Scan
from utils.s3_utils import get_top_dir_keys


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
NUM_CORES = 1       # [GENERAL] Number of cores (= number of MATLAB instances)

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


# Redis queue
try:
    config.set('redis', 'db', os.environ['REDIS_DB'])
except KeyError:
    pass                    # If the environment variable is not set this will fail -- fallback to poller.conf

redisman = RedisManager(host=config.get('redis','host'), db=config.get('redis','db'), port=config.get('redis','port'))

# AWS Resources:
aws_arns = dict()
aws_arns['statuslog'] = config.get('sns','topic')
sns = boto3.client('sns', region_name=config.get('sns', 'region'))

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
tasks, task = None, None        # Avoid annoying failure messages if these are not defined
childname = random.choice(config.get('offspring','offspring').split(','))


def announce(func, *args, **kwargs):
    '''
    Default decorator to send the current operation to the log
    '''
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
                    # !! DEPRECATED !!
                    # sendtoRabbitMQ(task)  # Send to the RVM queue
                    # !! DEPRECATED !!
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
    payload['hostname'] = socket.gethostname() + '-' + childname
    payload['ip'] = socket.gethostbyname(socket.gethostname())
    payload['message'] = message + ' (*)'
    payload['session_name'] = session_name

    try:
        _ = requests.post('http://{}/orchestrator'.format(config.get('rmq', 'hostname')), json=payload)
    except Exception as e:
        # If the boring machine is not available, just don't log. . .
        pass


@announce
def handleFailedTask(task):
    '''
    Behavior for failed tasks
    '''
    MAX_RETRIES = 9  # Under peek.lock conditions, Azure sets a default of 10, so let's catch before that

    if 'num_retries' in task.keys() and task['num_retries'] >= MAX_RETRIES:
        log('Max retries met for task, sending to DLQ: {}'.format(task))
        task['role'] = 'dlq'
        redisman.put(':'.join([task['role'], task['session_name']]), task)
    else:
        # Num_retries should exist, so this check is just for safety
        task['num_retries'] += 1
        log('Task FAILED. Re-enqueing: {}'.format(task), task['session_name'])

        # Delete error message and reenqueue
        del task['message']
        redisman.put(':'.join([task['role'], task['session_name']]), task)


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
def rebuildScanInfo(task):
    # S3 URIs
    session_dir = os.path.join(base_windows_path, task['session_name'])
    s3_result_path = '{}/results/farm_{}/block_{}/{}/'.format(task['clientid'], task['farm_name'].replace(' ',''), task['block_name'].replace(' ',''), task['session_name'])

    # If the session directory exists
    if os.path.exists(session_dir):
        # While the CSVs don't exist, just wait, someone must be making them
        while [os.path.exists(os.path.join(session_dir, 'videos', file)) for file in ['rvm.csv', 'vpr.csv']].count(False) != 0:
            time.sleep(10)

    # Else, create the directories and grab the CSVs
    else:
        try:
            os.makedirs(os.path.join(session_dir, 'videos', 'imu_basler'))
            os.makedirs(os.path.join(session_dir, 'results'))
            os.makedirs(os.path.join(session_dir ,'code'))
            os.makedirs(os.path.join(session_dir, 'csv'))
                
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
                for csvfile in ['rvm.csv', 'vpr.csv']:
                    s3r.Bucket(config.get('s3', 'bucket')).download_file(s3_result_path + csvfile, os.path.join(base_windows_path,
                                                                                 task['session_name'], 'videos', csvfile))
        except IOError as e:
            log('Rebuilding IO Error: {}'.format(e), task['session_name'])
            raise Exception(e)
            pass
        except Exception as e:
            log('A serious error has occurred rebuilding scan info: {}'.format(e), task['session_name'])
            raise Exception(e)

    # Ensure that a text file representing the session exists in the S3 session folder 
    # If it does not, create it and add the versions -- this should probably go with each task
    # but this works for now
    if os.name == 'nt':
        try:
            task['versions'] = {
                    'ScanOrchestrator': 
                        {'branch': subprocess.check_output(['git', '--git-dir', r'C:\AgriData\Projects\ScanOrchestrator\.git', 'rev-parse', '--symbolic-full-name', '--abbrev-ref', '@{u}'])[:-1],
                         'commit': subprocess.check_output(['git', '--git-dir', r'C:\AgriData\Projects\ScanOrchestrator\.git', 'rev-parse', 'HEAD'])[:-1]},
                    'MatlabCore':
                        {'branch': subprocess.check_output(['git', '--git-dir', r'C:\AgriData\Projects\MatlabCore\.git', 'rev-parse', '--symbolic-full-name', '--abbrev-ref', '@{u}'])[:-1],
                         'commit': subprocess.check_output(['git', '--git-dir', r'C:\AgriData\Projects\MatlabCore\.git', 'rev-parse', 'HEAD'])[:-1]}
                    }

            sessionfile = s3_result_path + 'session.json'
            if not 'Contents' in s3.list_objects(Bucket='agridatadepot', Prefix=sessionfile).keys():
                # Remove unnecessary, task specific information
                sessiondata = copy.deepcopy(task)           # Avoid editing task!
                for field in ['folders', 'result', 'session_name']:
                    if field in sessiondata['detection_params'].keys():
                        sessiondata['detection_params'].pop(field)
                for field in ['role', 'num_retries', 'tarfiles']:
                    if field in sessiondata.keys():
                        sessiondata.pop(field)
                data = StringIO(unicode(json.dumps(sessiondata, sort_keys=True, indent=4)))
                s3.put_object(Bucket=config.get('s3', 'bucket'), Key=sessionfile, Body=data.read())
        except Exception as e:
            log('An error occured while rebuilding: {}'.format(str(e)), task['session_name'])

@announce
def generateRVM(task):
    '''
    Generate a row video map.
    '''
    try:
        # Notify
        log('Received RVM task: {}'.format(task), task['session_name'])

        # Rebuild base scan info
        rebuildScanInfo(task)

        log('Calculating RVM', task['session_name'])
        mlab = matlabProcess()
        mlab.runTask(task, nargout=0)
        mlab.quit()

        # Generate tar files
        local_uri = os.path.join(base_windows_path, task['session_name'], 'videos', 'rvm.csv')
        data = pd.read_csv(local_uri, header=0)
        tarfiles = pd.Series.unique(data['file'])

        # Create and send tasks
        for task in [dict(task, tarfiles=[tf], num_retries=0, role='preproc') for tf in tarfiles]:
            redisman.put(':'.join([task['role'], task['session_name']]), task)

        log('RVM task complete', task['session_name'])
    except Exception as err:
        log('Failure on {}'.format(str(err)), task['session_name'])
        pass


@announce
def preprocess(task):
    '''
    Preprocessing method
    '''
    try:
        # Notify
        log('Received preprocessing task: {}'.format(task), task['session_name'])

        # Rebuild base scan info
        rebuildScanInfo(task)

        # Download the tarfiles and (pre-)preprocess them (there can be many not just one)
        video_dir = os.path.join(base_windows_path, task['session_name'], 'videos')
        for tar in task['tarfiles']:
            scanid = '_'.join(tar.split('_')[0:2])
            key = '{}/{}/{}'.format(task['clientid'], scanid, tar)
            log('Downloading {}'.format(key), task['session_name'])
            s3r.Bucket(config.get('s3', 'bucket')).download_file(key, os.path.join(video_dir, tar))

            # Untar
            tarfile.open(os.path.join(video_dir, tar)).extractall(
                path=os.path.join(video_dir, tar.replace('.tar.gz', '')))

            # Notify
            log('Preprocessing {}'.format(tar), task['session_name'])
           
            # Run the task
            mlab = matlabProcess()
            mlab.runTask(task, nargout=0)
            mlab.quit()

    except Exception as e:
        task['message'] = e
        handleFailedTask(task)
        pass


@announce
def detection(task):
    '''
    Detection method
    '''
    from deepLearning.infra import detect_s3_az

    try:
        if type(task) != dict:
            task = json.loads(task)
        log('Received detection task: {}'.format(task), task['session_name'])

        arg_list = []

        # The task may come in unicode -- let's change to regular string and format the command line arguments
        for k, v in task['detection_params'].iteritems():
            arg_list.append('--' + k)
            if type(v) == list:
                for vi in v:
                    arg_list.append(str(vi))
            else:
                arg_list.append(str(v))

        args_detect = detect_s3_az.parse_args(arg_list)
        logger.info(('detection process %r, %r' % (task, args_detect)))
        s3keys = detect_s3_az.main(args_detect)

        # for now we expect only one element
        assert (len(s3keys) <= 1)
        task['detection_params']['result'] = s3keys if not s3keys else s3keys[0]
        log('Success. Completed detection: {}'.format(task['detection_params']['result']), task['session_name'])

        # Pass to process
        task['role'] = 'process'
        redisman.put(':'.join([task['role'], task['session_name']]), task)

        # Remove output files
        for dir in glob.glob('/home/agridata/code/projects/deepLearning/infra/output/'):
            shutil.rmtree(dir)
    except Exception, e:
        tb = traceback.format_exc()
        logger.error(tb)
        task['message'] = str(tb)
        log('args, Task FAILED. Re-enqueueing... ({})'.format(task), task['session_name'])
        handleFailedTask(task)
        pass

@announce
def check_shapes(task):
    '''
    Fruit shape estimation will first check if the shape can be estimated. If not,
    we wait for a while before looking again
    '''
    pct_complete = 0.3          # Percentage required to produce a shape estimate
    timeout = 10                # In minutes (no need to keep attacking the queue)

    # Convert to useful dotdict
    task = dotdict(task)

    # Temp filed used as a semaphore for work in progress
    tempfile = '{}/results/farm_{}/block_{}/{}/fruit_size.temp'.format(task.clientid, task.farm_name.replace(' ', ''),task.block_name, task.session_name)

    try:
        # This condition is actually vaguely dependent on the similar one found in the process
        # method. This would catch some odd race cases
        if 'Contents' not in s3.list_objects(Bucket=config.get('s3', 'bucket'), Prefix=tempfile).keys():
            # Place the semaphore
            body = StringIO(unicode('Work In Progress'))
            s3.put_object(Bucket=config.get('s3', 'bucket'), Key=tempfile, Body=body.read())

            # Grab the list of available detection files
            extant = s3.list_objects(Bucket=config.get('s3', 'bucket'), Prefix='{}/results/farm_{}/block_{}/{}/detection/'.format(task.clientid, task.farm_name.replace(' ', ''),task.block_name, task.session_name))['Contents']
            
            # Parse filenames
            zips = [e['Key'].split('/')[-1] for e in extant if '.zip' in e['Key']]
            filerows =  list(np.unique([int(re.search('(?:row)([0-9]+)',z).group(1)) for z in zips]))

            # Obtain ground-truth RVM rows
            rvm = s3.get_object(Bucket=config.get('s3', 'bucket'), Key='{}/results/farm_{}/block_{}/{}/rvm.csv'.format(task.clientid, task.farm_name.replace(' ', ''),task.block_name, task.session_name))
            rvm = pd.read_csv(BytesIO(rvm['Body'].read()))
            rvmrows = list(np.unique(rvm['rows']))

            # Mean criteria - the mean of available rows must be approximately equal to the mean of the actual rows
            thresh = 0.1 * len(rvmrows)
            uniform = np.mean(rvmrows) - thresh <= np.mean(filerows) <= np.mean(rvmrows) + thresh

            # Percentage complete
            if len(rvmrows) <= 10:
                # If there are fewer than 10 rows, we require at least half the RVM rows represented
                complete = len(zips) >= len(rvm) * 0.5
            else:
                # Otherwise, we require a certain percentage (of the actual rows)
                complete = float(len(extant)) / float(len(rvm)) >= pct_complete

            # If the session is complete enough and the mean suggests it is a representative sample
            if complete and uniform:
                # Select random rows
                cadre = [file['Key'] for file in np.random.choice(extant, int(len(rvm) * pct_complete), replace=False)]
                cadre = cadre[0: min(len(cadre), 25)]
                # Notify
                log('Shape estimation beginning', task['session_name'])

                # Download frames and RVM
                video_dir = os.path.join(base_windows_path, task['session_name'], 'videos')
                if not os.path.exists(video_dir):
                    os.makedirs(video_dir)
                localrvm = os.path.join(video_dir, 'rvm.csv')
                if not os.path.exists(localrvm):
                    rvm.to_csv(localrvm, index=False)
                for zipfile in cadre:
                    log('Shape estimate downloading {}'.format(zipfile), task['session_name'])
                    s3r.Bucket(config.get('s3', 'bucket')).download_file(zipfile, os.path.join(video_dir, os.path.basename(zipfile)))

                # Convert to relative paths
                task.detection_params['folders'] = [os.path.basename(c) for c in cadre]

                # Run the task
                log('Estimating shapes', task['session_name'])
                mlab = matlabProcess()
                mlab.runTask(task, nargout=0)
                mlab.quit()
                log('Finished shape estimation', task['session_name'])

                # Remove the semaphore and return
                s3.delete_object(Bucket=config.get('s3','bucket'), Key=tempfile)

                # Re-enqueue
                redisman.put(':'.join(['process', task['session_name']]), task)
                return

            # Remove the semaphore (but don't return immediately; unfortunate redundancy)
            s3.delete_object(Bucket=config.get('s3','bucket'), Key=tempfile)

    except Exception as e:
        tb = traceback.format_exc()
        task['message'] = str(tb)
        log('Checkshape failed: ({})'.format(e, task['session_name']))
        task['role'] = 'process'
        # Remove the semaphore (but don't return immediately; unfortunate redundancy)
        s3.delete_object(Bucket=config.get('s3','bucket'), Key=tempfile)
        handleFailedTask(task)
        pass


@announce
def process(task):
    '''
    Processing method
    '''

    try:
        # Fruit size URIs
        fruituri = '{}/results/farm_{}/block_{}/{}/fruit_size.txt'.format(task['clientid'], task['farm_name'].replace(' ', ''),task['block_name'], task['session_name'])
        tempuri = '{}/results/farm_{}/block_{}/{}/fruit_size.temp'.format(task['clientid'], task['farm_name'].replace(' ', ''),task['block_name'], task['session_name'])

        # Do they exist?
        fruitfile = 'Contents' in s3.list_objects(Bucket=config.get('s3', 'bucket'), Prefix=fruituri)
        tempfile = 'Contents' in s3.list_objects(Bucket=config.get('s3', 'bucket'), Prefix=tempuri)

        if not fruitfile and not tempfile:          # If there are no fruit size related files, try and make them
            task['role'] = 'shapesize'
            check_shapes(task)

        while not fruitfile:                        # If there is a temp file, someone is making the fruit size file, so just wait
            time.sleep(60*3)
            fruitfile = 'Contents' in s3.list_objects(Bucket=config.get('s3', 'bucket'), Prefix=fruituri)
            
        # Proceeed with Process
        task['role'] = 'process'
        
        # Notify
        log('Received processing task: {}'.format(task, task['session_name']))

        # Reformat message if necessary
        if type(task) == unicode:
            # MATLAB seems to prefer to send strings with single quotes, which needs to be converted
            task = json.loads(task.replace("u'", "'").replace("'", '"'))
        # The detection params reult is sometimes a string and sometimes a list -- is this because of the above?
        if type(task['detection_params']['result']) == unicode:
            task['detection_params']['result'] = [task['detection_params']['result']]

        # Ensure role (this is repeated from the end of preprocess handoff. . .)
        task['role'] = 'process'

        # Rebuild base scan info
        rebuildScanInfo(task)

        # Download frames
        video_dir = os.path.join(base_windows_path, task['session_name'], 'videos')
        for zipfile in task['detection_params']['result']:
            log('Downloading {}'.format(zipfile), task['session_name'])
            s3r.Bucket(config.get('s3', 'bucket')).download_file(zipfile, os.path.join(video_dir, os.path.basename(zipfile)))

        # Run the task
        mlab = matlabProcess()
        mlab.runTask(task, nargout=0)
        mlab.quit()

    except Exception as e:
        tb = traceback.format_exc()
        logger.error(tb)
        task['message'] = str(tb)
        log('args, Task FAILED. Re-enqueueing... ({})'.format(task), task['session_name'])
        handleFailedTask(task)
        pass


@announce
def postprocess(task):
    '''
    Post-processing 
    '''
    # Notify
    log('Received postprocessing task: {}'.format(task, task['session_name']))

    # Convert to useful dotdict and set the role
    task = dotdict(task)

    try:
        sessionuri = '{}/results/farm_{}/block_{}/{}/'.format(task.clientid, task.farm_name.replace(' ', ''),task.block_name, task.session_name)
        session_results_top_level = get_top_dir_keys(s3, config.get('s3','bucket'), sessionuri)
        detection_results = len(s3.list_objects(Bucket=config.get('s3','bucket'), Prefix=sessionuri + 'detection/'))
        process_results = len(s3.list_objects(Bucket=config.get('s3','bucket'), Prefix=sessionuri + 'process-frames/'))
        summary = [k for k in session_results_top_level if k.lower().startswith('summary')]

        # For post process, we can use a few different thresholds
        # threshold = process_results == detection_results and not summary
        # threshold = not summary
        threshold = process_results >= (detection_results - 5) and not summary

        if threshold or task.get('is_manual', False):
            # Run
            mlab = matlabProcess()
            mlab.runTask(task, nargout=0)
            mlab.quit()

        # The implicit consequence of the above is that process step is not complete, so this task can now
        # simply die without any further action

    except:
        tb = traceback.format_exc()
        logger.error(tb)
        task['message'] = str(tb)
        log('args, Task FAILED. Re-enqueueing... ({})'.format(task), task['session_name'])
        handleFailedTask(task)
        pass


@announce
def identifyRole():
    '''
    This method is called when an instance starts. Based on its ComputerInfo, it will identify itself as a
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
    Start MATLAB engine and return a MATLAB instance
    '''
    logger.info('Starting MATLAB. . .')
    mlab = matlab.engine.start_matlab()
    mlab.addpath(mlab.genpath(startpath))

    return mlab


@announce
def getComputerInfoString():
    # Grab computer info
    ret = subprocess.check_output('ipconfig')

    # Format
    ignore = [' . ', '\r', '\n']
    for i in ignore:
        ret = ret.replace(i, '')

    return ret


@announce
def client(roles):
    '''
    Since Windows machines can perform either preprocessing / processing equally, one strategy is to have any computer
    perform one of these tasks
    '''
    timeout = 180        # This timeout is used to reduce strain on the server
    logger.info(('The redis db param used is ', config.get('redis', 'db')))
    while True:
        try:
            for role in roles:
                ns = role[0]
                for q in redisman.list_queues(ns):
                    if not redisman.empty(q):
                        task = redisman.get(q)
                        ### ENV VARIABLE HERE
                        task['db'] = config.get('redis', 'db')
                        role[1](task)
            time.sleep(timeout)
        except Exception as e:
            # Not sure what types of exceptions we'll get yet
            tb=traceback.print_exc()
            log('Redis error: {}, traceback:{}'.format(e, tb))


def run(args):
    '''
    The main entry point
    '''
    try:
        role = args.role or os.name

        log('I\'m awake! db: {}'.format(config.get('redis','db')))

        # Specialty roles up front
        # Scan filename / log file conversion
        if role == 'convert':
            for scan in Scan.objects():
                convert(scan)

         # AWS poller (for new uploads)
        elif role == 'poll':
            poll()

        # RVM / Preprocessing / Processing
        elif role in ['nt', 'rvm', 'preproc', 'process', 'postprocess']:
            workers = list()
            for worker in xrange(NUM_CORES):
                p = multiprocess.Process(target=client, args=[[('rvm', generateRVM), ('preproc', preprocess), ('process', process), ('postprocess',postprocess)]])
                workers.append(p)
                p.start()

                # Stagger MATLABs
                time.sleep(10)

            for worker in workers:
                p.join()

        # Detection
        elif role in ['posix', 'detection']:
            client([('detection',detection)])

        # Unknown
        else:
            log('Could not determine role type.\n{}'.format(getComputerInfoString),task['session_name'])

    # Overarching error
    except Exception as e:
        log(str(e), task['session_name'])


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
    args = parse_args()
    run(args)
