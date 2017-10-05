import yaml
import datetime
import glob
from utils.models_min import *
from utils.connection import *
from pprint import pprint

farm = Farm.objects.get(name='Promontory')
cameras = {'22179656' : 'Left', '22179659' : 'Right'}

tasks = glob.glob('tasks/*.yaml')
for taskfile in tasks:
    with open(taskfile,'r') as task:
        task = yaml.load(task)
        block = Block.objects.get(name=str(task['block_name']), farm=farm.id)
        block.update(num_rows = len(block.rows))

        for scanid in task['include_scans']:
            scan = Scan.objects.get(scanid=scanid)
            scan.update(blocks = [block.id])

            scan.update(farm = farm.id)

            # Direction
            if task['block_name'] in ['2', '3', '11A', '17C', '21']:
                scan.update(direction = -1)
            else:
                scan.update(direction = 1)

            # Cameras
            scan.update(cameras = cameras)