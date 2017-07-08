#!/usr/bin/python

import ConfigParser
import copy
import json
import logging
import subprocess
import sys
import os
import time

import boto3
import numpy as np
from mongoengine import connect
from pymongo import MongoClient

# AgriData
from models_min import *
from connection import *

# Operational parameters
WAIT_TIME = 20  # AWS wait time for messages
NUM_MSGS = 10  # Number of messages to grab at a time
RETRY_DELAY = 60  # Number of seconds to wait upon encountering an error

# Load config file
config = ConfigParser.ConfigParser()
config.read('poller.conf')

# Temporary location for collateral in processing
tmpdir = '/tmp/agridata/'
if not os.path.exists(tmpdir):
    os.mkdirs(tmpdir)

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
        4) Reupload
    '''

    ## Rename tars
    # %his can be done without downloading any of the .tar.gz files, although they will
    # be downloaded later. This process is expected to take some time, as resource on S3 
    # cannot be moved but must be copied
    s3base = '{}/{}'.format(str(scan.client), str(scan.id))
    for fidx, file in enumerate(s3.list_objects(Bucket=config.get('s3','bucket'), Prefix=s3base)['Contents']):
        if str(scan.client) in file['Key'] and '.tar.gz' in file['Key']:

            # Ksplit is: [0] clientid; [-2==1] scanid; [-1==2] camera
            ksplit = file['Key'].split('/')
            scanid = ksplit[-2]
            logging.info('[{}/{}, {}/{}] Converting s3://{}/{}'.format(pidx,len(list(page_iterator)),fidx,len(page['Contents']),bucket,file['Key']))
            s3resource.Object(bucket, ksplit[0] + '/' + scanid + '/' + scanid + '_' + ksplit[-1]).copy_from(CopySource=config.get('s3','bucket')+'/'+file['Key'])
            logging.info('--> {}'.format(ksplit[0] + '/' + scanid + '/' + scanid + '_' + ksplit[-1]))

            # Here is the delete command, so use wisely
            # s3resource.Object(bucket,file['Key']).delete()

    ## Download files required for tar inspection and CSV update
    # Temporary file destination
    dest = os.path.join(tmpdr, '{}/{}'.format(str(scan.client), str(scan.scanid)))
    if not os.path.exists(os.path.join(tmpdir,dest)):
        mkdirs(dest)

    # Downlaod to scan folder
    subprocess.call(['rclone','-v',
                     '--config', '{}/config/rclone/rclone.conf'.format(config.get('env','home')),
                     'copy', 'AgriDataS3:{}/{}/{}/'.format(config.get('s3','bucket'), str(client.id), str(scan.scanid)),
                     dest])

    ## Rename CSVs by prepending the scanid (22179658_09_07.tar.gz becomes 2017-06-28_08-51_22179658_09_07.tar.gz)

    ## Add 'filenames' column to CSV

    ## Upload 

def initiateScanProcess(scan):
    '''
    This is the entry point to the processing pipeline. It takes a raw, newly uploaded scan and 
    handles some essential tasks that are required before processing.
    '''

    ## Check for formatting
    # These are the keys (files) in the scan directory
    s3base = '{}/{}'.format(str(scan.client), str(scan.id))
    keys = [obj['Key'] for obj in s3.list_objects(Bucket=config.get('s3','bucket'), Prefix=s3base)['Contents']]

    # If there are any files that don't start with a scanid, they must be of the old format, so format them.
    if not len([k for k in keys if k.split('/')[-1].startswith(scanid)]):
        transformScan(scan)

    # The scan is uploaded and ready to be placed in the 'ready' or 'rvm' queue, where it 
    # may be be evaluated for 'goodness'

def sendMsgToAzure(queue, msg):
    # Here, we send a message to an azure service bus


if __name__ == '__main__':
    ## Poller
    # poll()

    ## Manual scan
    for scan in Scan.objects():
        initiateScanProcess(scan)