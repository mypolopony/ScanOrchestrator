import glob
import os
import pandas as pd
import tarfile

clientdir = '/Volumes/warmstorage/AgriData/594ce94f1fb3590bef00c927'
scans = glob.glob('/Volumes/warmstorage/AgriData/594ce94f1fb3590bef00c927/*')
cameras = ['22179658', '22179674']

for scandir in scans:
    scanid = scandir.split('/')[-1]

    for camera in cameras:
        log = pd.read_csv(scandir + '/' + camera + '_' + scanid + '.csv')
        newlog = scandir + '/' + scanid + '_' + camera + '.csv'

        if not os.path.exists(newlog):

            names = list()

            try:
                print('Analyzing {}'.format(scanid))
                for tf in sorted(glob.glob(scandir + '/' + camera + '*.tar.gz')):
                    print(set(names))
                    for i in range(0,len(tarfile.open(tf).getmembers())):
                        names.append(tf.split('/')[-1])

                if len(names) == len(log) + 1:
                    print('Shaving one')
                    names = names[:-1]
                if len(names) == len(log) - 1:
                    print('Adding one')
                    names.append(names[-1])

                log['filename'] = names
                log.to_csv(newlog)
            except Exception as e:
                print('\n *** Failed: {} {}'.format(camera, scandir))
                print(' *** names = {}, log = {}'.format(len(names), len(log)))
                try:
                    log['filename'] = names[:len(log)]
                except:
                    log = log[:len(names)]
                    log['filename'] = names
                log.to_csv(newlog)
                continue
        else:
            print('Skipping existing: {}'.format(newlog))