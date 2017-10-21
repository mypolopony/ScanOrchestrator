import os
import datetime
from __init__ import RedisManager
import ConfigParser
import time
from pprint import pprint

# Load config file
config = ConfigParser.ConfigParser()
config_parent_dir = '.'
config_path = os.path.join(config_parent_dir, 'utils', 'poller.conf')
config.read(config_path)

redisman = RedisManager(host=config.get('redis','host'), db=os.getenv('REDIS_DB'), port=config.get('redis','port'))

if __name__ == '__main__':
    while True:
        print('-----------')
        print(datetime.datetime.strftime(datetime.datetime.now(),'%c'))
        status = redisman.status()
        pprint(status)
        print('Total: {}'.format(sum([val for val in status.itervalues()])))
        time.sleep(10)
