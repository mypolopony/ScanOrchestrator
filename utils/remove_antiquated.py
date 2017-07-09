'''
This file meant to remove files with the old filename convention. It uses a pure S3
method, as opposed to the database method
'''

from header import *
import re

# Enumerate real clients
paginator = s3.get_paginator('list_objects')
clients = s3.list_objects(Bucket='agridatadepot', Delimiter='/').get('CommonPrefixes')
clients = [str(c['Prefix']) for c in clients if len(c['Prefix']) == 25 and '000' not in c['Prefix']]

# Safe check, if each old file has a corresponding new file, delete it
pattern_cam = re.compile('[0-9]{8}')                  # Camera pattern
for client in clients:
    scans = [obj['Prefix'] for obj in s3.list_objects(Bucket='agridatadepot', Prefix=client, Delimiter='/')['CommonPrefixes']]
    scans = [s.split('/')[1] for s in scans]

    for scan in scans:
        scanfiles = [obj['Key'].replace(client,'') for obj in s3.list_objects(Bucket='agridatadepot', Prefix='{}{}'.format(client, scan))['Contents']]
        oldfiles = [f for f in scanfiles if 'tar.gz' in f and not f.split('/')[1].startswith(scan)]

        for oldfile in oldfiles:
            camera = re.search(pattern_cam, oldfile).group()
            time = '_'.join([oldfile.split('_')[-2], oldfile.split('_')[-1]])
            newfile = '{}/{}_{}_{}'.format(scan,scan,camera,time)
            if newfile in scanfiles:
                print('Delete: {} ({} exists)'.format(oldfile, newfile))
                s3r.Object('agridatadepot','{}{}'.format(client,oldfile)).delete()

        oldfiles = [f for f in scanfiles if 'csv' in f and not f.split('/')[1].startswith(scan) and 'results' not in f and 'debug' not in f]
        for oldfile in oldfiles:
            camera = re.search(pattern_cam, oldfile).group()
            newfile = '{}/{}_{}.csv'.format(scan,scan,camera)
            if newfile in scanfiles:
                print('Delete: {} ({} exists)'.format(oldfile, newfile))
                s3r.Object('agridatadepot','{}{}'.format(client,oldfile)).delete()