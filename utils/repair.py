from models_min import *
from connection import *
from kombu import Connection, Exchange, Queue, Producer
import parse
import glob
import datetime
import pandas as pd
import boto3
import yaml
import os
import csv
import ConfigParser

import datetime
from __init__ import RedisManager
import ConfigParser
import time


# Load config file
config = ConfigParser.ConfigParser()
config_path = '/Users/mypolopony/Projects/ScanOrchestrator/utils/poller.conf'
config.read(config_path)

# S3 Resources
S3Key = config.get('s3', 'aws_access_key_id')
S3Secret = config.get('s3', 'aws_secret_access_key')
s3 = boto3.client('s3', aws_access_key_id=S3Key, aws_secret_access_key=S3Secret)
s3r = boto3.resource('s3', aws_access_key_id=S3Key, aws_secret_access_key=S3Secret)

# Redis connection
redisman = RedisManager(host=config.get('redis','host'), db=config.get('redis', 'db'), port=config.get('redis','port'))

# Bucket
bucket = 'agridatadepot'

# Dry run
execute = True



def toPreprocess(lost):
    return {'num_retries': 0,
            'session_name': lost['session_name'],
            'farmid': lost['farmid'],
            'block_name': lost['block_name'],
            'blockid': lost['blockid'],
            'clientid': lost['clientid'],
            'farm_name': lost['farm_name'],
            'test': lost['test'],
            'role': 'preprocess',
            'scanids': lost['scanids'],
            'tarfiles': lost['file'],
            'detection_params': lost['detection_params']}


def toDetection(lost):
    return {'num_retries': 0,
            'farmid': lost['farmid'],
            'block_name': lost['block_name'],
            'blockid': lost['blockid'],
            'detection_params': {
                'base_url_path': '{}/results/farm_{}/block_{}/{}'.format(lost['clientid'],lost['farm_name'],lost['block_name'],lost['session_name']),
                's3_aws_secret_access_key': S3Secret,
                'input_path': 'preprocess-frames',
                's3_aws_access_key_id': S3Key,
                'folders': os.path.basename(lost['uri']),
                'caffemodel_s3_url_trunk': lost['detection_params']['caffemodel_s3_url_trunk'],
                'bucket': bucket,
                'caffemodel_s3_url_cluster': lost['detection_params']['caffemodel_s3_url_cluster'],
                'output_path': 'detection',
                'session_name': datetime.datetime.strftime(datetime.datetime.now(),'%m%d%H%M%S')},
            'farm_name': lost['farm_name'],
            'test': lost['test'],
            'role': 'detection',
            'clientid': lost['clientid'],
            'scanids': lost['scanids'],
            'session_name': lost['session_name'],
            'tarfiles': lost['file']}


def toProcess(lost):
    # Model
    uri = lost['detect_uri']

    lost = toDetection(lost)

    # Ack!
    lost['detection_params']['result'] = [uri]
    lost['role'] = 'process'

    return lost


def insert(tasks):
    print('Inserting {} {} tasks'.format(len(tasks),tasks[0]['role']))

    if execute:
        for task in tasks:
            # Convert True / False into JSON-appropriate int
            if task['test']:
                task['test'] = 1
            else:
                task['test'] = 0
            redisman.put(':'.join([task['role'], task['session_name']]), task)
    else:
        print('{} ---> {}'.format(len(tasks), ':'.join([tasks[0]['role'], tasks[0]['session_name']])))

    '''
    # Assume tasks are all destined for the same exchange
    ex = Exchange(tasks[0]['role'], type='topic', channel=chan)
    producer = Producer(channel=chan, exchange=ex)

    if execute:
        for task in tasks:
            Queue('_'.join([task['role'], task['session_name']]), exchange=ex,
              channel=chan, routing_key=task['session_name']).declare()
            producer.publish(task, routing_key=task['session_name'])
    '''


def repair(task):
    if not os.path.exists('temp'):
        os.mkdir('temp')

    # Change to dict
    task = yaml.load(open(taskfile, 'r'))
    task = Task(client_name=task['client_name'],
                farm_name=task['farm_name'],
                block_name=task['block_name'],
                session_name=task['session_name'],
                caffemodel_s3_url_cluster=task['detection_params']['caffemodel_s3_url_cluster'],
                caffemodel_s3_url_trunk=task['detection_params']['caffemodel_s3_url_trunk'],
                test=task['test'],
                exclude_scans=task['exclude_scans'],
                include_scans=task['include_scans'],
                role=task['role'])
    task = dotdict(task.to_json())
    task.farm_name = task.farm_name.replace(' ', '')
    task.block_name = task.block_name.replace(' ', '')

    subtasks = dict()

    # Download RVM
    rvmuri_str = '{clientid}/results/farm_{farm_name}/block_{block_name}/{session_name}/rvm.csv'.format(clientid=task.clientid, farm_name = task.farm_name, block_name = task.block_name, session_name = task.session_name)
    s3r.Bucket(bucket).download_file(rvmuri_str, 'temp/rvm.csv')

    # Read RVM
    subtasks['rvm'] = list()
    with open('temp/rvm.csv') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            row['direction'] = row['direction'].replace('dir','')
            row.update(task)

            fileparse = parse.parse('{scandate}_{scantime}_{camera}_{hour}_{minute}.tar.gz', row['file']).__dict__['named']
            row.update(fileparse)

            subtasks['rvm'].append(dotdict(row))

    #######
    #######

    try:
        # Preprocess (on s3)
        extant = s3.list_objects(Bucket=bucket,Prefix='{}/results/farm_{}/block_{}/{}/preprocess-frames/'.format(task.clientid, task.farm_name.replace(' ', ''),task.block_name, task.session_name))['Contents']
        extant = [f['Key'] for f in extant]

        # Preprocess (expected)
        subtasks['preprocess'] = list()
        toadd = list()
        for subtask in subtasks['rvm']:
            if subtask['of'] == '1':
                subtask.uri = '{clientid}/results/farm_{farm_name}/block_{block_name}/{session_name}/preprocess-frames/{scanid}_{camera}_{hour}_{minute}-preprocess-row{row}-dir{direction}.zip'.format(clientid=subtask.clientid, farm_name=subtask.farm_name, block_name=subtask.block_name, session_name=subtask.session_name,scanid=subtask.scanid,camera=subtask.camera,hour=subtask.hour, minute=subtask.minute, row=subtask.rows, direction=subtask.direction)
            else:
                subtask.uri = '{clientid}/results/farm_{farm_name}/block_{block_name}/{session_name}/preprocess-frames/{scanid}_{camera}_{hour}_{minute}-preprocess-row{row}-dir{direction}-{part}of{of}.zip'.format(clientid=subtask.clientid, farm_name=subtask.farm_name, block_name=subtask.block_name, session_name=subtask.session_name,scanid=subtask.scanid,camera=subtask.camera,hour=subtask.hour, minute=subtask.minute, row=subtask.rows, direction=subtask.direction, part=subtask.part, of=subtask.of)
            subtasks['preprocess'].append(subtask)

            if subtask.uri not in extant:
                toadd.append(subtask)

        # Inject missing preprocess
        if toadd:
            # pass *!override!* skip over preprocess
            insert([toPreprocess(a) for a in toadd])
        else:
            print('Preprocess all set')
    except Exception as e:
        print('Process failed: {}'.format(e))

    #######
    #######

    try:
        # Detection (on s3)
        extant = s3.list_objects(Bucket=bucket,Prefix='{}/results/farm_{}/block_{}/{}/detection/'.format(task.clientid, task.farm_name.replace(' ', ''),task.block_name, task.session_name))['Contents']
        extant = [f['Key'] for f in extant]

        # Detection (expected (from preprocess))
        subtasks['detection'] = list()
        toadd = list()
        for subtask in subtasks['preprocess']:
            if subtask['of'] == '1':
                subtask.uri = '{clientid}/results/farm_{farm_name}/block_{block_name}/{session_name}/detection/{scanid}_{camera}_{hour}_{minute}-preprocess-row{row}-dir{direction}.zip'.format(clientid=subtask.clientid, farm_name=subtask.farm_name, block_name=subtask.block_name, session_name=subtask.session_name,scanid=subtask.scanid,camera=subtask.camera,hour=subtask.hour, minute=subtask.minute, row=subtask.rows, direction=subtask.direction)
            else:
                subtask.uri = '{clientid}/results/farm_{farm_name}/block_{block_name}/{session_name}/detection/{scanid}_{camera}_{hour}_{minute}-preprocess-row{row}-dir{direction}-{part}of{of}.zip'.format(clientid=subtask.clientid, farm_name=subtask.farm_name, block_name=subtask.block_name, session_name=subtask.session_name,scanid=subtask.scanid,camera=subtask.camera,hour=subtask.hour, minute=subtask.minute, row=subtask.rows, direction=subtask.direction, part=subtask.part, of=subtask.of)

            # This is a horrible hack to save the detection uri
            subtask.detect_uri = subtask.uri

            subtask['role'] = 'detection'
            subtasks['detection'].append(subtask)

            if subtask.uri not in extant:
                toadd.append(subtask)

        # Inject missing detection
        if toadd:
            insert([toDetection(a) for a in toadd])
        else:
            print('Detection all set')
    except Exception as e:
        print('Detection failed: {}'.format(e))


    #######
    #######

    try:
        # Process (on s3)
        extant = s3.list_objects(Bucket=bucket,Prefix='{}/results/farm_{}/block_{}/{}/process-frames/'.format(task.clientid, task.farm_name.replace(' ', ''),task.block_name, task.session_name))['Contents']
        extant = [f['Key'] for f in extant]

        # Process (expected (from detection))
        subtasks['process'] = list()
        toadd = list()
        for subtask in subtasks['detection']:
            if subtask['of'] == '1':
                subtask.uri = '{clientid}/results/farm_{farm_name}/block_{block_name}/{session_name}/process-frames/row{row}-dir{direction}-{scanid}_{camera}_{hour}_{minute}.zip'.format(clientid=subtask.clientid, farm_name=subtask.farm_name, block_name=subtask.block_name, session_name=subtask.session_name,scanid=subtask.scanid,camera=subtask.camera,hour=subtask.hour, minute=subtask.minute, row=subtask.rows, direction=subtask.direction)
            else:
                subtask.uri = '{clientid}/results/farm_{farm_name}/block_{block_name}/{session_name}/process-frames/row{row}-dir{direction}-part{part}of{of}-{scanid}_{camera}_{hour}_{minute}.zip'.format(clientid=subtask.clientid, farm_name=subtask.farm_name, block_name=subtask.block_name, session_name=subtask.session_name,scanid=subtask.scanid,camera=subtask.camera,hour=subtask.hour, minute=subtask.minute, row=subtask.rows, direction=subtask.direction, part=subtask.part, of=subtask.of)

            if subtask.uri not in extant:
                toadd.append(subtask)

        # Inject missing processing
        if toadd:
            insert([toProcess(a) for a in toadd])
        else:
            print('Process all set')
    except Exception as e:
        print('Process failed: {}'.format(e))


if __name__ == '__main__':
    for taskfile in glob.glob('/Users/mypolopony/Projects/ScanOrchestrator/tasks/*.yaml'):
        print('\nAssessing {}'.format(taskfile.split('/')[-1].replace('.yaml','')))
        repair(taskfile)