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
import socket
import traceback
import requests
import multiprocess

import pandas as pd
import numpy as np

from bson.objectid import ObjectId

# AgriData
from utils.models_min import *
from utils.connection import *

# Operational parameters
WAIT_TIME = 20      # AWS wait time for messages
NUM_MSGS = 10       # Number of messages to grab at a time
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
bus_service = ServiceBusService(service_namespace='agridataqueues',
                                shared_access_key_name='sharedaccess',
                                shared_access_key_value='cWonhEE3LIQ2cqf49mAL2uIZPV/Ig85YnyBtdb1z+xo=')


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
                raise e

            # For each result. . .
            logger.info('Received {} tasks'.format(len(results)))
            for ridx, result in enumerate(results):
                try:
                    logger.info('Queueing task {}/{}'.format(ridx, len(results)))
                    task = handleAWSMessage(result)             # Parse clientid and scanid
                    task['role'] = 'rvm'                        # Tag with the first task step
                    sendtoServiceBus('rvm',json.dumps(task))    # Send to the RVM queue
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
    for csv in glob.glob(dest + '/{}*.csv'.format(scan.scanid)):
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
    task process is responsible for re-enqueing the task if it fails. This 
    '''
    incoming = None
    while not incoming:
    	incoming = bus_service.receive_queue_message(queue, peek_lock=lock).body

    # Here, we can accept either properly formatted JSON strings or, if necessary, convert the
    # quotes to make them compliant.
    try:
        msg = json.loads(incoming)
    except:
        msg = json.loads(incoming.replace("'",'"').replace('u"','"'))

    return msg

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
        boringmachine = '54.164.89.210'
        r = requests.post('http://{}:5000/orchestrator'.format(boringmachine), json = payload)
    except Exception as e:
        logger.warning('Boringmachine not reachable: {}'.format(e))

@announce
def generateRVM():
    '''
    Given a set of scans (or one scan), generate a row video map.
    '''
    
    # Start MATLAB
    mlab = matlabProcess()

    while True:
        try:
            log('Waiting for task')
            task = receivefromServiceBus('rvm')
            log('Received task')

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
            staged = db.staging.find({'block': scan.blocks[0]})
            if staged:
                task['scanids'] = task['scanids'] + [s['_id'] for s in staged]

            # Send the arguments off to batch_auto, return is the S3 location of rvm.csvs
            s3uri, localuri = mlab.runTask('rvm', task['clientid'], task['scanids'])
            task['rvm_uri'] = s3uri

            # Check for completeness
            data = pd.read_csv(localuri, header=0)
            rows_found = len(set([(r, d) for r,d in zip(data['rows'],data['direction'])])) / 2
            if rows_found < block.num_rows * 0.5:
                emitSNSMessage('RVM is not long enough (found {}, expected {})! [{}]'.format(rows_found, block.num_rows, task))
            elif rows_found > block.num_rows:
                emitSNSMessage('Too many rows found (found {}, expected {})! [{}]'.format(rows_found, block.num_rows, task))
            else:
                # Split tarfiles
                tarfiles = pd.Series.unique(data['file'])
                for shard in np.array_split(tarfiles, int(len(tarfiles)/6)):
                    task['tarfiles'] = list(shard)
                    sendtoServiceBus('preprocess', task)

                log('Task Complete: {}'.format(json.dumps(task)))

        except Exception as err:
            emitSNSMessage('Failure on {}'.format(str(err)))
            pass


@announce
def preprocess():
    '''
    Preprocessing method
    '''
    # Here is the number of children to spawn
    NUM_MATLAB_INSTANCES = 4

    # Canonical filepath
    video_dir = r'C:\AgriData\Projects\videos'
    if os.path.exists(video_dir):
        shutil.rmtree(video_dir)
    os.makedirs(video_dir)

    while True:
        try:
            log('Waiting for task')
            task = receivefromServiceBus('preprocess')
            log('Received task')

            # Download the tarfiles
            for tar in task['tarfiles']:
                scanid = '_'.join(tar.split('_')[0:2])
                key = '{}/{}/{}'.format(task['clientid'], scanid, tar)
                log('Downloading {}'.format(key))
                try:
                    s3r.Bucket(config.get('s3','bucket')).download_file(key, os.path.join(video_dir, tar))
                except Exception as e:
                    log('Download of {} has resulted in an error: {}'.format(key, e))

            # Only need one matlab process to untar
            mlab = matlabProcess()
            mlab.my_untar(video_dir,  nargout=0)
            mlab.quit()

            # Download the RVM
            key = '/'.join(task['rvm_uri'].split('/')[3:])
            s3r.Bucket(config.get('s3', 'bucket')).download_file(key, os.path.join(video_dir, key.split('/')[-1]))

            # These are the processes to be spawned. They call to the launchMatlabTasks wrapper primarily
            # because the multiprocessing library could not directly be called as some of the objects were
            # not pickleable? The multiprocess library (notice the spelling) overcomes this, so I don't think
            # the function wrapper is necessary anymore
            log('Starting work')
            workers = list()
            for instance in range(NUM_MATLAB_INSTANCES):
                worker = multiprocess.Process(target=launchMatlabTasks, args=['preprocess', task])
                worker.start()
                workers.append(worker)

            for worker in workers:
                worker.join()

            log('All MATLAB instances have finished. . . Uploading zip files')

            # Pre file upload, recreate relevant parts of analysis_struct
            analysis_struct = dict.fromkeys(['video_folder', 's3_result_path'])
            analysis_struct['video_folder'] = video_dir

            # S3 results path
            # TODO: Replace with last directory based on environment (local / temp / selly)
            analysis_struct['s3_result_path'] = 's3://agridatadepot.s3.amazonaws.com/{}/results/farm_{}/block_{}/selly'.format(task['clientid'], task['farmname'], task['blockname'])
            
            mlab = matlabProcess()
            mlab.upload_logs(analysis_struct, 1, nargout=0)
            mlab.quit()

            # What goes here? Hand-off to detection -- where are the .zip files?
            log('Success')
            print(task)
            for zipfile in glob.glob(analysis_struct['video_folder'] + '/*.zip'):
                detectiontask = task
                detectiontask['detection_params'] =  dict(
                    bucket='deeplearning_data',
                    base_url_path='{}/results/{}/block_{}/temp'.format(task['clientid'],task['farmname'].replace(' ',''), task['blockname']),
                    input_path='preprocess-frames',
                    output_path='detection',
                    caffemodel_s3_url_cluster='s3://deeplearning_data/models/best/post-bloom_july_13_2017_224000.caffemodel',
                    caffemodel_s3_url_trunk='s3://deeplearning_data/models/best/trunk_june_10_400000.caffemodel',
                    s3_aws_access_key_id='AKIAJC7XVEAQELBKAANQ',
                    s3_aws_secret_access_key='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',
                    session_name= datetime.datetime.now().strftime('%m-%d-%H-%M-%S'),
                    folders=[ zipfile ]
                    )
                sendtoServiceBus('detection', detectiontask)
        except Exception as e:
            task['message'] = e
            emitSNSMessage('Task FAILED: {}'.format(task))
            pass

@announce 
def launchMatlabTasks(taskname, task):
    '''
    A separate wrapper for multiple matlabs. It is called by multiprocess, which has the capability to
    pickle (actually, dill) a wider range of objects (like function wrappers)
    '''
    mlab = matlabProcess()
    mlab.runTask(taskname, task['clientid'], task['scanids'])
    mlab.quit()


@announce
def detection(scan):
    '''
    Detection method
    '''
    print('Koshyland')

    # Grab a task
    task = receivefromServiceBus('detection')

    while task:
        # Do things
        x = 0

    emitSNSMessage('== Task Complete: {}'.format(json.dumps(task)))


@announce
def process(scan):
    '''
    Processing method
    '''

    # Start MATLAB
    mlab = matlabProcess()

    emitSNSMessage('== Task Complete: {}'.format(json.dumps(task)))


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
    logger.info('[fnord!] Starting MATLAB. . .')
    mlab = matlab.engine.start_matlab()
    mlab.addpath(mlab.genpath(startpath))
    #  mlab.javaaddpath(r'C:\AgriData\Projects\MatlabCore\extern\mongo\mongo-java-driver-3.4.2.jar');

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


if __name__ == '__main__':
    # Initial decision points. It is important that these do not return anything. This requires that each task stream
    # be responsible for handling its own end conditions, whether it be an graceful exit, an abrupt termination, etc. The
    # reason is that that task itself has the sole responsibility of knowing what it should or should not be doing and how
    # to handle adverse or successful events.

    # What task are we meant to do? This is based on instance names
    roletype = identifyRole()
    log('I''m awake! Roletype is {}'.format(roletype))

    try:
        # Preprocessing
        if 'preproc' in roletype:
            preprocess()

        # Convert scan filenames and CSVs from old style to new style
        elif roletype == 'convert':
            scans = Scan.objects()

            for scan in scans:
                try:
                    initiateScanProcess(scan)
                except Exception as e:
                    logger.error(traceback.print_exc())
                    logger.info('An error has occured: {}'.format(e))

        # Daemon mode
        elif roletype == 'poll':
            poll()

        # RVM Generation
        elif 'rvm' in roletype or 'jumpbox' in roletype:
            generateRVM()

        # Detection
        elif roletype == 'detection':
            detection()

        # Error
        else:
            emitSNSMessage('Could not determine role type.\n{}'.format(getComputerInfoString))
    except Exception as e:
        emitSNSMessage(str(e))
