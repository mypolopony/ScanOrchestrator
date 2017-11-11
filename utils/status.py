# -*- coding: utf-8 -*-
# @Author: Selwyn-Lloyd McPherson
# @Date:   2017-10-30 02:04:55
# @Last Modified by:   Selwyn-Lloyd McPherson
# @Last Modified time: 2017-11-05 20:38:02
# @C/O: AgriData, Inc.

import os
import datetime
import ConfigParser
import time
import json
from pprint import pprint
from __init__ import RedisManager

# Load config file
config = ConfigParser.ConfigParser()
config_parent_dir = '.'
config_path = os.path.join(config_parent_dir, 'utils', 'poller.conf')
config.read(config_path)

redisman = RedisManager(host=config.get('redis','host'), db=os.getenv('REDIS_DB', 0), port=config.get('redis','port'))
outfile = None
# outfile = '/Users/mypolopony/Projects/ScanOrchestrator/tasks/logs/october29.promontory'

if __name__ == '__main__':
    while True:
        if outfile:
            with open(outfile, 'w+') as logfile:
                logfile.write('----------\n')
                logfile.write(datetime.datetime.strftime(datetime.datetime.now(),'%c\n'))
                status = redisman.status()
                logfile.write(json.dumps(status))
                logfile.write('\n')
                logfile.write('Total: {}\n\n'.format(sum([val for val in status.itervalues()])))
                time.sleep(10)
        else:
            print('----------\n')
            print(datetime.datetime.strftime(datetime.datetime.now(),'%c\n'))
            status = redisman.status()
            print(json.dumps(status))
            print('Total: {}'.format(sum([val for val in status.itervalues()])))
            time.sleep(10)