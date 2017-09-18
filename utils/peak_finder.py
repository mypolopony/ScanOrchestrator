import os
from datetime import datetime
import time
from pprint import pprint
import boto3
import ConfigParser
import numpy as np
import pandas as pd
import glob
import peakutils
import matplotlib.pyplot as plt

config = ConfigParser.ConfigParser()
config.read('/Users/mypolopony/Projects/ScanOrchestrator/utils/poller.conf')

# AWS Resources: S3
S3Key = config.get('s3', 'aws_access_key_id')
S3Secret = config.get('s3', 'aws_secret_access_key')
s3 = boto3.client('s3', aws_access_key_id=S3Key, aws_secret_access_key=S3Secret)
s3r = boto3.resource('s3', aws_access_key_id=S3Key, aws_secret_access_key=S3Secret)
s3bucket = 'agridatadepot'

# Constants
FPS = 20

scans = ['2017-09-09_07-47',
'2017-09-09_08-48',
'2017-09-09_09-54',
'2017-09-09_12-08',
'2017-09-09_13-05',
'2017-09-09_13-28',
'2017-09-09_14-03',
'2017-09-09_14-40',
'2017-09-09_14-53',
'2017-09-09_15-11',
'2017-09-10_08-04',
'2017-09-10_08-17',
'2017-09-10_08-59',
'2017-09-10_09-18',
'2017-09-10_10-02',
'2017-09-10_11-20',
'2017-09-10_12-01',
'2017-09-10_12-05',
'2017-09-10_12-20']

for scan in scans:
    try:
        os.makedirs(scan)
    except OSError:
        pass
    objects = s3.list_objects(Bucket=s3bucket, Prefix='58f5e3c21fb35955235c7b31/{}/'.format(scan))
    objects = [o['Key'] for o in objects['Contents'] if 'csv' in o['Key']]
    for o in objects:
        o = os.path.basename(o)
        savepath = '{}/{}'.format(scan, o)
        if not os.path.exists(savepath):
            print('Downloading {}'.format(savepath))
            s3r.Bucket(s3bucket).download_file('58f5e3c21fb35955235c7b31/{}/{}'.format(scan, o), savepath)

for scan in scans:
    print(scan)
    for csv in glob.glob('{}/*.csv'.format(scan)):
        data = pd.read_csv(csv)
        col = data['IMU_COMPASS_X']
        coldiff = list()
        factor = 7
        for idx, coli in enumerate(col):
            if idx < FPS*factor:
                coldiff.append(0)
            else:
                coldiff.append(coli-col[idx-FPS*(factor-1)])

        coldiff = -np.array(coldiff)

        indexes = peakutils.indexes(coldiff, thres=0.1/max(coldiff), min_dist=1500)
        print('\t{} peaks = {} rows from {}'.format(len(indexes), len(indexes) * 2, csv))

        plt.figure()
        plt.plot(col)
        plt.plot(coldiff, color='b', linestyle='dashed', linewidth=0.2)
        plt.vlines(indexes,min(col), max(col), colors='r')
        plt.savefig(csv.replace('.csv',''))
