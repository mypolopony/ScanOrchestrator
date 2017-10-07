from models_min import *
from connection import *
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
config_path = './utils/poller.conf'
config.read(config_path)

# S3 Resources
S3Key = config.get('s3', 'aws_access_key_id')
S3Secret = config.get('s3', 'aws_secret_access_key')
s3 = boto3.client('s3', aws_access_key_id=S3Key, aws_secret_access_key=S3Secret)
s3r = boto3.resource('s3', aws_access_key_id=S3Key, aws_secret_access_key=S3Secret)

#set the redis/db param from the environment
config.set('redis', 'db', os.environ['REDIS_DB'])
# Redis connection
redisman = RedisManager(host=config.get('redis','host'), db=config.get('redis', 'db'), port=config.get('redis','port'))

# Bucket
bucket = 'agridatadepot'

# Execute?
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
            'role': 'preproc',
            'scanids': lost['scanids'],
            'tarfiles': [lost['file']],
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
                'folders': [os.path.basename(lost['uri'])],
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
            'tarfiles': [lost['file']]}


def toProcess(lost):
    # Model
    uri = lost['detect_uri']

    # Generate detection
    lost = toDetection(lost)

    # Modify for process
    lost['detection_params']['result'] = [uri]
    lost['detection_params']['folders'] = [os.path.basename(uri)]
    lost['role'] = 'process'

    return lost

def toPostProcess(lost):
    # Reuse detection
    lost['uri'] = lost['detection_params']['folders']       # Must recreate this otherwise ephemeral bit to reuse detection :(
    lost = toDetection(lost)

    # Change role
    lost['role'] = 'postprocess'

    return lost


def insert(tasks):
    if execute:
        print('Inserting {} {} tasks'.format(len(tasks),tasks[0]['role']))
        for task in tasks:
            # Convert True / False into JSON-appropriate int
            if task['test']:
                task['test'] = 1
            else:
                task['test'] = 0
            redisman.put(':'.join([task['role'], task['session_name']]), task)
    else:
        print('(dry run) {} ---> {}'.format(len(tasks), ':'.join([tasks[0]['role'], tasks[0]['session_name']])))


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

    #################
    # To preprocess #
    #################

    try:
        # Preprocess (on s3)
        try:
            extant = s3.list_objects(Bucket=bucket,Prefix='{}/results/farm_{}/block_{}/{}/preprocess-frames/'.format(task.clientid, task.farm_name.replace(' ', ''),task.block_name, task.session_name))['Contents']
            extant = [f['Key'] for f in extant]
        except KeyError:
            extant = list()

        # Preprocess (expected)
        subtasks['preproc'] = list()
        toadd = list()
        for subtask in subtasks['rvm']:
            if subtask['of'] == '1':
                subtask.uri = '{clientid}/results/farm_{farm_name}/block_{block_name}/{session_name}/preprocess-frames/{scanid}_{camera}_{hour}_{minute}-preprocess-row{row}-dir{direction}.zip'.format(clientid=subtask.clientid, farm_name=subtask.farm_name, block_name=subtask.block_name, session_name=subtask.session_name,scanid=subtask.scanid,camera=subtask.camera,hour=subtask.hour, minute=subtask.minute, row=subtask.rows, direction=subtask.direction)
            else:
                subtask.uri = '{clientid}/results/farm_{farm_name}/block_{block_name}/{session_name}/preprocess-frames/{scanid}_{camera}_{hour}_{minute}-preprocess-row{row}-dir{direction}-{part}of{of}.zip'.format(clientid=subtask.clientid, farm_name=subtask.farm_name, block_name=subtask.block_name, session_name=subtask.session_name,scanid=subtask.scanid,camera=subtask.camera,hour=subtask.hour, minute=subtask.minute, row=subtask.rows, direction=subtask.direction, part=subtask.part, of=subtask.of)

            if subtask.uri not in extant:
                toadd.append(subtask)
            else:
                subtasks['preproc'].append(subtask)

        # Inject missing preprocess
        if toadd:
            insert([toPreprocess(a) for a in toadd])
            # print('Skipping preprocess')
        else:
            print('Preprocess all set')
    except Exception as e:
        print('Process failed: {}'.format(e))

    ################
    # To detection #
    ################

    try:
        # Detection (on s3)
        try:
            extant = s3.list_objects(Bucket=bucket,Prefix='{}/results/farm_{}/block_{}/{}/detection/'.format(task.clientid, task.farm_name.replace(' ', ''),task.block_name, task.session_name))['Contents']
            extant = [f['Key'] for f in extant]
        except KeyError:
            extant = list()

        # Detection (expected (from preprocess))
        subtasks['detection'] = list()
        toadd = list()
        for subtask in subtasks['preproc']:
            if subtask['of'] == '1':
                subtask.uri = '{clientid}/results/farm_{farm_name}/block_{block_name}/{session_name}/detection/{scanid}_{camera}_{hour}_{minute}-preprocess-row{row}-dir{direction}.zip'.format(clientid=subtask.clientid, farm_name=subtask.farm_name, block_name=subtask.block_name, session_name=subtask.session_name,scanid=subtask.scanid,camera=subtask.camera,hour=subtask.hour, minute=subtask.minute, row=subtask.rows, direction=subtask.direction)
            else:
                subtask.uri = '{clientid}/results/farm_{farm_name}/block_{block_name}/{session_name}/detection/{scanid}_{camera}_{hour}_{minute}-preprocess-row{row}-dir{direction}-{part}of{of}.zip'.format(clientid=subtask.clientid, farm_name=subtask.farm_name, block_name=subtask.block_name, session_name=subtask.session_name,scanid=subtask.scanid,camera=subtask.camera,hour=subtask.hour, minute=subtask.minute, row=subtask.rows, direction=subtask.direction, part=subtask.part, of=subtask.of)

            # This is a horrible hack to save the detection uri
            subtask.detect_uri = subtask.uri

            subtask['role'] = 'detection'
            
            if subtask.uri not in extant:
                toadd.append(subtask)
            else:
                subtasks['detection'].append(subtask)

        # Inject missing detection
        if toadd:
            insert([toDetection(a) for a in toadd])
            # print('Skipping detectopm')
        else:
            print('Detection all set')
    except Exception as e:
        print('Detection failed: {}'.format(e))


    ##############
    # To process #
    ##############

    try:
        # Process (on s3)
        try:
            extant = s3.list_objects(Bucket=bucket,Prefix='{}/results/farm_{}/block_{}/{}/process-frames/'.format(task.clientid, task.farm_name.replace(' ', ''),task.block_name, task.session_name))['Contents']
            extant = [f['Key'] for f in extant]
        except KeyError:
            extant = list()

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
            else:
                subtasks['process'].append(subtask)

        # Inject missing processing
        if toadd:
            insert([toProcess(a) for a in toadd])
            # print('Skipping processing')
        else:
            print('Process all set')
    except Exception as e:
        print('Process failed: {}'.format(e))

    ##################
    # To postprocess #
    ##################

    try:
        sessionuri = '{}/results/farm_{}/block_{}/{}/'.format(task.clientid, task.farm_name.replace(' ', ''),task.block_name, task.session_name)
        session_results = s3.list_objects(Bucket=config.get('s3','bucket'), Prefix=sessionuri)
        summary = [k['Key'] for k in session_results['Contents'] if 'summary' in k['Key']]

        # Only postprocess remains
        if len(subtasks['detection']) == len(subtasks['process']) and not summary:
            # Grab an exemplary process task
            task = subtasks['process'][0]
            # Change role
            task['role'] = 'postprocess'
            # And insert
            insert([task])
        else:
            print('Postprocess all set')
    except Exception as e:
        print('Postprocess failed: {}'.format(e))


if __name__ == '__main__':
    print('\n db used is:', config.get('redis', 'db'))
    for taskfile in glob.glob('tasks/*.yaml'):
        try:
            print('\nAssessing {}'.format(taskfile.split('/')[-1].replace('.yaml','')))
            repair(taskfile)
        except Exception as e:
            print('Failed: {}'.format(e))