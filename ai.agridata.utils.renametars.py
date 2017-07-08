from pymongo import MongoClient
from datetime import datetime
from functools import wraps
from dateutil import parser
import glob
import random
import string
import pandas as pd
import numpy as np
import boto3
import re
import pickle


# Boto
session = boto3.Session(profile_name='old')
s3 = session.client('s3')
s3resource = session.resource('s3')
paginator = s3.get_paginator('list_objects')
clientid = '594ce94f1fb3590bef00c927'
bucket = 'agridatadepot'


page_iterator = paginator.paginate(Bucket=bucket, Prefix=clientid)
for pidx, page in enumerate(page_iterator):
    for fidx,file in enumerate(page['Contents']):
        if clientid in file['Key'] and '.tar.gz' in file['Key'] in file['Key']:
            ksplit = file['Key'].split('/')
            scanid = file['Key'].split('/')[-2]
            if file['Key'].count(scanid) == 1:
                print('[{}/{}, {}/{}] Converting s3://{}/{}'.format(pidx,len(list(page_iterator)),fidx,len(page['Contents']),bucket,file['Key']))
                s3resource.Object(bucket, ksplit[0] + '/' + scanid + '/' + scanid + '_' + ksplit[-1]).copy_from(CopySource=bucket+'/'+file['Key'])
                print('--> {}'.format(ksplit[0] + '/' + scanid + '/' + scanid + '_' + ksplit[-1]))
                s3resource.Object(bucket,file['Key']).delete()
            else:
                print('Skipping s3://{}/{}'.format(bucket,file['Key']))

'''
page_iterator = paginator.paginate(Bucket=bucket, Prefix=clientid)
for pidx, page in enumerate(page_iterator):
    for fidx,file in enumerate(page['Contents']):
        if clientid in file['Key'] and '.tar.gz' in file['Key']:
            ksplit = file['Key'].split('/')
            scanid = file['Key'].split('/')[-2]
            if file['Key'].count(scanid) == 1:
                print('[{}/{}, {}/{}] Converting s3://{}/{}'.format(pidx,len(list(page_iterator)),fidx,len(page['Contents']),bucket,file['Key']))
                s3resource.Object(bucket, ksplit[0] + '/' + scanid + '/' + scanid + '_' + ksplit[-1]).copy_from(CopySource=bucket+'/'+file['Key'])
                print('--> {}'.format(ksplit[0] + '/' + scanid + '/' + scanid + '_' + ksplit[-1]))
                s3resource.Object(bucket,file['Key']).delete()
            else:
                print('Skipping s3://{}/{}'.format(bucket,file['Key']))
'''

'''
page_iterator = paginator.paginate(Bucket=bucket, Prefix='59055036037c2fc5e372ad9d/results/farm_Ranch541/block_3')
for pidx, page in enumerate(page_iterator):
    for fidx,file in enumerate(page['Contents']):
        if 'block_3_' in file['Key']:
            print('[{}/{}, {}/{}] Converting s3://{}/{}'.format(pidx,len(list(page_iterator)),fidx,len(page['Contents']),bucket,file['Key']))
            s3resource.Object(bucket, file['Key'].replace('block_3_','')).copy_from(CopySource=bucket+'/'+file['Key'])
            print('--> {}'.format(file['Key'].replace('block_3_','')))
            s3resource.Object(bucket,file['Key']).delete()
        else:
            print('Skipping s3://{}/{}'.format(bucket,file['Key']))
'''

'''
b = s3resource.Bucket(bucket)
page_iterator = paginator.paginate(Bucket=bucket, Prefix=clientid)
for pidx, page in enumerate(page_iterator):
    for fidx,file in enumerate(page['Contents']):
        if '2017-06-27_10-02' in file['Key']:
            print(file['Key'])
            scanid = file['Key'].split('/')[-2]
            if '.csv' in file['Key'] and 'results' not in file['Key'] and file['Key'].split('/')[-1].index('_') == 8:
                print(file['Key'])
                ksplit = file['Key'].split('/')
                obj = b.Object(file['Key'])
                name = file['Key'].split('/')[-1]
                s3.download_file(bucket, file['Key'], name)
                upframe = pd.read_csv(name)
                if 'filenames' in upframe.columns:
                    upframe['filename'] = [scanid + '_' + i for i in upframe['filenames']]
                    upframe.drop('filenames')
                else:
                    upframe['filename'] = [scanid + '_' + i for i in upframe['filename']]
                upframe.to_csv('upload.csv',index=False)
                up = name
                print('\t{}'.format(up))
                # s3.upload_file('upload.csv', bucket, clientid + '/' + '_'.join(up.split('_')[1:3]).replace('.csv','') + '/' + '_'.join(up.split('_')[1:3]).replace('.csv','') +'_' + up.split('_')[0] + '.csv')
'''

'''
with open('s3.tmp', 'wb+') as data:
    obj.download_fileobj(data)
    txtdata = data.read()
digits = "".join( [random.choice(string.digits) for i in xrange(8)] )
chars = "".join( [random.choice(string.letters) for i in xrange(15)] )
name = file['Key'].split('/')[-1]
s3.download_file(bucket, file['Key'], name)
'''
