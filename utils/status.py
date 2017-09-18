import os
import datetime
from __init__ import RedisManager
import ConfigParser
import time
# Load config file
config = ConfigParser.ConfigParser()
config_parent_dir = '.'
config_path = os.path.join(config_parent_dir, 'utils', 'poller.conf')
config.read(config_path)

redisman = RedisManager(host=config.get('redis','host'), db=config.get('redis', 'db'), port=config.get('redis','port'))

if __name__ == '__main__':
    while True:
        print('\n--------\n{}'.format(datetime.datetime.strftime(datetime.datetime.now(),'%c')))
        redisman.status()
        time.sleep(10)