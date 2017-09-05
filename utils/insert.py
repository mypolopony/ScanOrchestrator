import pickle
import os
from kombu import Connection
from datetime import datetime
import time
import re
from pprint import pprint
import boto3
import re
import ConfigParser
import numpy as np

config = ConfigParser.ConfigParser()
config.read('utils/poller.conf')

# AWS Resources: S3
S3Key = config.get('s3', 'aws_access_key_id')
S3Secret = config.get('s3', 'aws_secret_access_key')
s3 = boto3.client('s3', aws_access_key_id=S3Key, aws_secret_access_key=S3Secret)
s3r = boto3.resource('s3', aws_access_key_id=S3Key, aws_secret_access_key=S3Secret)
s3bucket = 'agridatadepot'

def receivefromRabbitMQ(queue):
    msgs = list()
    with Connection('amqp://{}:{}@{}:5672//'.format('agridata', 'agridata', 'azmaster')) as kbu:
        q = kbu.SimpleQueue(queue)
        print(q.qsize())
        for i in xrange(q.qsize()):
            message = q.get()
            message.ack()
            msgs.append(message.payload)
        q.close()
    return msgs


def sendtoRabbitMQ(queue, message):
    with Connection('amqp://{}:{}@{}:5672//'.format('agridata', 'agridata', 'azmaster')) as kbu:
        q = kbu.SimpleQueue(queue)
        q.put(message)
        q.close()


def toProcess(lost):
    print('Inserting {} tasks'.format(len(lost)))
    target = 'process_{}'.format(sessionname)
    base = {'blockid': blockid,
            'blockname': blockname,
            'clientid': clientid,
            'detection_params': {'base_url_path': s3prefix,
                                 'bucket': s3bucket,
                                 'caffemodel_s3_url_cluster': 's3://deeplearning_data/models/best/cluster_june_15_288000.caffemodel',
                                 'caffemodel_s3_url_trunk': 's3://deeplearning_data/models/best/trunk_june_10_400000.caffemodel',
                                 'folders': None,
                                 'input_path': 'preprocess-frames',
                                 'output_path': 'detection',
                                 'result': None,
                                 's3_aws_access_key_id': 'AKIAJC7XVEAQELBKAANQ',
                                 's3_aws_secret_access_key': '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',
                                 'session_name': time.strftime('%m-%d-%H-%M-%S')},
            'farmid': farmid,
            'farmname': farmname,
            'num_retries': 0,
            'role': 'process',
            'scanids': None,
            'session_name': sessionname,
            'tarfiles': None,
            'test': 0}

    for l in lost:
        parts = l.replace('part', '').split('-')
        resultname = '-'.join([parts[3], parts[4], parts[5], parts[6]]).replace('.zip', '') + '-preprocess-' + '-'.join(
            [parts[0], parts[1], parts[2]])
        base['tarfiles'] = [resultname[0:31] + '.tar.gz']
        base['detection_params']['folders'] = [resultname + '.zip']
        base['detection_params']['result'] = [s3prefix + '/detection/' + resultname + '.zip']
        base['scanids'] = scanids

        pprint(base)
        if execute:
            sendtoRabbitMQ(target, base)


def toPreprocess(lost):
    if type(lost) is not list():
        lost = [lost]
    print('Inserting {} tasks'.format(len(lost)))
    target = 'preprocess_{}'.format(sessionname)
    base = {'num_retries': 0,
            'session_name': sessionname,
            'farmid': farmid,
            'blockname': blockname,
            'blockid': blockid,
            'clientid': clientid,
            'farmname': farmname,
            'test': False,
            'role': 'preprocess',
            'scanids': scanids,
            'tarfiles': lost,
            'detection_params': {
                'caffemodel_s3_url_cluster': 's3://deeplearning_data/models/best/cluster_june_15_288000.caffemodel',
                'caffemodel_s3_url_trunk': 's3://deeplearning_data/models/best/trunk_june_10_400000.caffemodel'
            }
    }

    pprint(base)
    if execute:
        sendtoRabbitMQ(target, base)


## SETUP
blockname = '4D'
blockid = '599f7ac855f30b2756ca2b5a'
sessionname = '4d2'
farmname = 'Quatacker-Burns'
farmid = '5994c6ab55f30b158613c517'
clientid = '591daa81e1cf4d8cbfbb1bf6'
scanids = ['2017-08-17_09-47','2017-08-17_10-25','2017-08-17_11-23','2017-08-17_12-07','2017-08-17_12-33']
s3prefix = '{}/results/farm_{}/block_{}/{}'.format(clientid, farmname, blockname, sessionname)
execute = True

preprocess = s3.list_objects(Bucket=s3bucket, Prefix=s3prefix + '/preprocess-frames')['Contents']
preprocess = [os.path.basename(p['Key']) for p in preprocess]

detection = s3.list_objects(Bucket=s3bucket, Prefix=s3prefix + '/detection')['Contents']
detection = [os.path.basename(d['Key']) for d in detection if '.zip' in d['Key']]

process = s3.list_objects(Bucket=s3bucket, Prefix=s3prefix + '/process-frames')['Contents']
process = [os.path.basename(p['Key']) for p in process if '.zip' in p['Key']]

# What happens if there is part1of1? Is that a thing that can happen?
#det2proc = [re.search('row[0-9]+',f).group() + re.search('-dir[0-9]', f).group() + '-part' + re.search('[0-9]+of[0-9]+',f).group() + '-' + f[0:31] + '.zip' for f in detection if 'SUN' not in f]
#lost = list(set(det2proc) - set(process))
#print(lost)

#toProcess(lost)


# Preprocess diff tars
# a = [re.match('2017[^p]+',obj['Key'].split('/')[-1]).group()[:-1]+'.tar.gz' for obj in  s3.list_objects(Bucket=s3bucket, Prefix='{}/results/farm_{}/block_{}/{}/preprocess-frames'.format(clientid, farmname, blockname, '4d'))['Contents']]
# b = [re.match('2017[^p]+',obj['Key'].split('/')[-1]).group()[:-1]+'.tar.gz' for obj in  s3.list_objects(Bucket=s3bucket, Prefix='{}/results/farm_{}/block_{}/{}/preprocess-frames'.format(clientid, farmname, blockname, '4d2'))['Contents']]
# tarfiles = set(a) - set(b)

# Preprocess diff rows/directions
# a = [obj['Key'].split('/')[-1] for obj in s3.list_objects(Bucket=s3bucket, Prefix='{}/results/farm_{}/block_{}/{}/preprocess-frames'.format(clientid, farmname, blockname, '4d'))['Contents']]
# b = [obj['Key'].split('/')[-1] for obj in s3.list_objects(Bucket=s3bucket, Prefix='{}/results/farm_{}/block_{}/{}/preprocess-frames'.format(clientid, farmname, blockname, '4d2'))['Contents']]


# Selective Preprocess
#tarfiles = ['2017-08-17_13-59_22005516_14_00.tar.gz', '2017-08-17_13-59_22005516_14_01.tar.gz', '2017-08-17_13-59_22005516_14_02.tar.gz', '2017-08-17_13-59_22005516_14_02.tar.gz', '2017-08-17_13-59_22005516_14_03.tar.gz', '2017-08-17_13-59_22005516_14_03.tar.gz', '2017-08-17_13-59_22005516_14_04.tar.gz', '2017-08-17_13-59_22005516_14_05.tar.gz', '2017-08-17_13-59_22005516_14_05.tar.gz', '2017-08-17_13-59_22005516_14_06.tar.gz', '2017-08-17_13-59_22005516_14_06.tar.gz', '2017-08-17_13-59_22005516_14_07.tar.gz', '2017-08-17_13-59_22005516_14_07.tar.gz', '2017-08-17_13-59_22005516_14_08.tar.gz', '2017-08-17_13-59_22005516_14_09.tar.gz', '2017-08-17_13-59_22005516_14_09.tar.gz', '2017-08-17_13-59_22005516_14_10.tar.gz', '2017-08-17_13-59_22005516_14_10.tar.gz', '2017-08-17_13-59_22005516_14_11.tar.gz', '2017-08-17_13-59_22005516_14_11.tar.gz', '2017-08-17_13-59_22005516_14_12.tar.gz', '2017-08-17_13-59_22005516_14_12.tar.gz', '2017-08-17_13-59_22005516_14_13.tar.gz', '2017-08-17_13-59_22005516_14_13.tar.gz', '2017-08-17_13-59_22005516_14_14.tar.gz', '2017-08-17_13-59_22005516_14_15.tar.gz', '2017-08-17_13-59_22005516_14_16.tar.gz', '2017-08-17_13-59_22005516_14_16.tar.gz', '2017-08-17_13-59_22005516_14_17.tar.gz', '2017-08-17_13-59_22005516_14_17.tar.gz', '2017-08-17_13-59_22005516_14_18.tar.gz', '2017-08-17_13-59_22005516_14_18.tar.gz', '2017-08-17_13-59_22005516_14_19.tar.gz', '2017-08-17_13-59_22005516_14_19.tar.gz', '2017-08-17_13-59_22005516_14_20.tar.gz', '2017-08-17_13-59_22005516_14_20.tar.gz', '2017-08-17_13-59_22005516_14_21.tar.gz', '2017-08-17_13-59_22005516_14_22.tar.gz', '2017-08-17_13-59_22005516_14_23.tar.gz', '2017-08-17_13-59_22005516_14_23.tar.gz', '2017-08-17_13-59_22005516_14_24.tar.gz', '2017-08-17_13-59_22005516_14_24.tar.gz', '2017-08-17_13-59_22005516_14_25.tar.gz', '2017-08-17_13-59_22005520_13_59.tar.gz', '2017-08-17_13-59_22005520_14_00.tar.gz', '2017-08-17_13-59_22005520_14_00.tar.gz', '2017-08-17_13-59_22005520_14_01.tar.gz', '2017-08-17_13-59_22005520_14_02.tar.gz', '2017-08-17_13-59_22005520_14_02.tar.gz', '2017-08-17_13-59_22005520_14_03.tar.gz', '2017-08-17_13-59_22005520_14_03.tar.gz', '2017-08-17_13-59_22005520_14_04.tar.gz', '2017-08-17_13-59_22005520_14_05.tar.gz', '2017-08-17_13-59_22005520_14_05.tar.gz', '2017-08-17_13-59_22005520_14_06.tar.gz', '2017-08-17_13-59_22005520_14_06.tar.gz', '2017-08-17_13-59_22005520_14_07.tar.gz', '2017-08-17_13-59_22005520_14_07.tar.gz', '2017-08-17_13-59_22005520_14_08.tar.gz', '2017-08-17_13-59_22005520_14_09.tar.gz', '2017-08-17_13-59_22005520_14_09.tar.gz', '2017-08-17_13-59_22005520_14_10.tar.gz', '2017-08-17_13-59_22005520_14_10.tar.gz', '2017-08-17_13-59_22005520_14_11.tar.gz', '2017-08-17_13-59_22005520_14_11.tar.gz', '2017-08-17_13-59_22005520_14_12.tar.gz', '2017-08-17_13-59_22005520_14_12.tar.gz', '2017-08-17_13-59_22005520_14_13.tar.gz', '2017-08-17_13-59_22005520_14_13.tar.gz', '2017-08-17_13-59_22005520_14_14.tar.gz', '2017-08-17_13-59_22005520_14_15.tar.gz', '2017-08-17_13-59_22005520_14_16.tar.gz', '2017-08-17_13-59_22005520_14_16.tar.gz', '2017-08-17_13-59_22005520_14_17.tar.gz', '2017-08-17_13-59_22005520_14_17.tar.gz', '2017-08-17_13-59_22005520_14_18.tar.gz', '2017-08-17_13-59_22005520_14_18.tar.gz', '2017-08-17_13-59_22005520_14_19.tar.gz', '2017-08-17_13-59_22005520_14_19.tar.gz', '2017-08-17_13-59_22005520_14_20.tar.gz', '2017-08-17_13-59_22005520_14_20.tar.gz', '2017-08-17_13-59_22005520_14_21.tar.gz', '2017-08-17_13-59_22005520_14_22.tar.gz', '2017-08-17_13-59_22005520_14_23.tar.gz', '2017-08-17_13-59_22005520_14_23.tar.gz', '2017-08-17_13-59_22005520_14_24.tar.gz', '2017-08-17_13-59_22005520_14_24.tar.gz', '2017-08-17_13-59_22005520_14_25.tar.gz']
tarfiles = ['2017-08-17_09-47_22005516_09_51.tar.gz','2017-08-17_09-47_22005516_09_53.tar.gz','2017-08-17_09-47_22005516_09_55.tar.gz','2017-08-17_10-25_22005516_10_32.tar.gz','2017-08-17_10-25_22005516_10_35.tar.gz','2017-08-17_10-25_22005516_10_36.tar.gz','2017-08-17_10-25_22005516_10_37.tar.gz','2017-08-17_10-25_22005516_10_40.tar.gz','2017-08-17_10-25_22005516_10_41.tar.gz','2017-08-17_10-25_22005516_10_42.tar.gz','2017-08-17_10-25_22005516_10_44.tar.gz','2017-08-17_10-25_22005516_10_49.tar.gz','2017-08-17_10-25_22005516_10_53.tar.gz','2017-08-17_10-25_22005516_10_54.tar.gz','2017-08-17_10-25_22005516_10_56.tar.gz','2017-08-17_10-25_22005516_11_00.tar.gz','2017-08-17_10-25_22005516_11_07.tar.gz','2017-08-17_12-33_22005516_13_01.tar.gz','2017-08-17_12-33_22005516_13_09.tar.gz','2017-08-17_12-33_22005516_13_32.tar.gz']
tarfiles = [list(tf) for tf in np.array_split(list(tarfiles), len(tarfiles) / 4)]
for group in tarfiles[0]:
    toPreprocess(group)


# detection = [obj['Key'].split('/')[-1] for obj in s3.list_objects(Bucket='agridatadepot',Prefix='591daa81e1cf4d8cbfbb1bf6/results/farm_Quatacker-Burns/block_3C/3c/detection/')['Contents']]
# det2proc = [re.search('row[0-9]+',f).group() + re.search('-dir[0-9]', f).group() + '-part' + re.search('[0-9]+of[0-9]+',f).group() + '-' + f[0:31] + '.zip' for f in detection if 'SUN' not in f]

# toProcess(det2proc)
