#!/usr/bin/python

import boto3
import json
import time
import math
import sys
import socket
import ConfigParser
import logging
import cv2
import urllib
import infra

from pprint import pprint
from datetime import datetime
from weather import Weather
import pandas as pd
import numpy as np
from dateutil import parser
from mongoengine import connect
from pymongo import MongoClient
import subprocess

# AgriData
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
WAIT_TIME = 20          # AWS wait time for messages
NUM_MSGS = 10           # Number of messages to grab at a time
RETRY_DELAY = 60        # Number of seconds to wait upon encountering an error

# Load config file
config = ConfigParser.ConfigParser()
config.read('poller.conf')

# AWS Resources: S3
S3Key = config.get('s3', 'aws_access_key_id')
S3Secret = config.get('s3','aws_secret_access_key')
s3 = boto3.client('s3', aws_access_key_id=S3Key, aws_secret_access_key=S3Secret)

# AWS Resources: SQS
SQSKey = config.get('sqs','aws_access_key_id')
SQSSecret = config.get('sqs','aws_secret_access_key')
SQSQueueName = config.get('sqs', 'queue_name')
SQSQueueRegion = config.get('sqs', 'region')
sqsr = boto3.resource('sqs', aws_access_key_id=SQSKey, aws_secret_access_key=SQSSecret, region_name=SQSQueueRegion)
queue = sqsr.get_queue_by_name(QueueName='new-upload-queue')


# Initializing logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]: %(message)s', datefmt='%a, %d %b %Y %H:%M:%S')

def run():
    start = datetime.datetime.now()

    logging.info('Amazon AWS SQS Poller\n\n')

    # Poll messages
    # while True:
    try:
        logging.info('Requesting tasks')
        try:
            results = queue.receive_messages(MaxNumberOfMessages=NUM_MSGS,WaitTimeSeconds=WAIT_TIME)
        except Exception as e:
            logging.error('A problem occured: {}'.str(e))
            raise(e)

        # For each result. . .
        for result in results:
            logging.info('Received {} tasks'.format(len(results)))
            for ridx, result in enumerate(results):
                try:
                    logging.info('Handling message {}/{}'.format(ridx,len(results)))
                    handleMessage(result)
                except:
                    logging.exception('Error while handling messge: {}'.format(result.body))
                    # can add dead letter queue for poisonous messages here
        else:
            logging.info('No tasks to do.')

    except Exception as e:
        # General errors, with a retry delay
        logging.exception('Unexpected error: {}. Sleeping for {} seconds'.format(str(e),RETRY_DELAY))

    logging.info('Sleeping for {} seconds'.format(RETRY_DELAY))
    time.sleep(RETRY_DELAY)

def preprocess(obj):
    '''
    Preprocess
    '''
    fact = 33

    logging.info('\t\tThe ninth degree of {} is {}'.format(fact, int(math.factorial(fact))))


def detection(obj):
    '''
    Detection
    '''
    weather = Weather()

    city = 'San Francisco'
    location = weather.lookup_by_location(city)
    condition = location.condition()

    logging.info('The current weather in {}:'.format(city))
    logging.info(' :: '.join([condition['date'],condition['temp'],condition['text']]))


def process(obj):
    '''
    Processs
    '''

    goal = 1116

    # Calculate
    sequence = list()
    cursor = 2
    while cursor <= goal:
        for i in range(2, cursor):
            if cursor % i == 0:
                cursor = cursor + 1
        sequence.append(cursor)
        cursor += 1

    logging.info('Here\'s the Fibonacci Sequence to {}:\n\t{}'.format(goal,sequence.__str__()))


def postprocess(obj, payload):
    '''
    Postpostprocess
    '''

    logging.info('\tDownloading. . .')
    for key, value in payload.iteritems():
        logging.info('\t{}: {}'.format(key, value))

    img = url_to_image(payload['uri'])
    average_color = np.average(np.average(img, axis=0), axis=0)
    hexcolor = [c.encode('hex') for c in map(chr, map(int, average_color))]
    colorpage = 'http://www.color-hex.com/color/' + ''.join(hexcolor)
    logging.info('\tAverage RGB color of photo: {} [{}]'.format(average_color, colorpage))


def url_to_image(url):
    '''
    Download an image, convert it to a NumPy array, and then read it into OpenCV format
    '''

    resp = urllib.urlopen(url)
    image = np.asarray(bytearray(resp.read()), dtype="uint8")
    image = cv2.imdecode(image, cv2.IMREAD_COLOR)

    return image


def execute(cmd):
  do_instances_win.py -n dummy_session -g matlab_status


def handleMessage(result):
    # Weurd double string to JSON
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

    # Delete task
    # When tasks are received, they are temporarily marked as 'taken' but it is up to this process to actually
    # delete them or, failing that, default to releasing them back to the queue


    # Demo payload
    img_payload = {'name': 'Cyrille Habis',
                   'filename': 'Cyrille-Habis.jpg',
                   'uri': 'http://lebnet.us/wp-content/uploads/2016/04/Cyrille-Habis.jpg'}

if __name__ == '__main__':
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


'''
:: Appendix B: Sample tasks


    # 1] Pre-process goes here
    logging.info('\n\nPre-processing (client {}, scanid {}, file {})'.format(task['clientid'], task['scanid'], task['filename']))
    preprocess(task)

    # 2] Detection goes here
    logging.info('\n\nRunning detection (client {}, scanid {}, file {})'.format(task['clientid'], task['scanid'], task['filename']))
    detection(task)

    # 3] Process goes here
    logging.info('\n\nProcessing (client {}, scanid {}, file {})'.format(task['clientid'], task['scanid'], task['filename']))
    process(task)

    # 4] Post-process goes here
    logging.info('\n\nPost-processing (client {}, scanid {}, file {})'.format(task['clientid'], task['scanid'], task['filename']))
    postprocess(task, img_payload)

'''