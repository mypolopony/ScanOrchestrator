import argparse

from connection import *
from models_min import *

def parse_args():
    '''
    Add other parameters here
    '''
    parser=argparse.ArgumentParser('orchestrator')
    parser.add_argument('-c', '--client', help='client name', dest='client_name', type=int, default=None)

    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = parse_args()

    if args.client_name:
        clients = [Client.objects.get(name=args.client_name)]
    else:
        clients = Client.objects({})

    for client in clients:
        base_url = 's3://agridatadepot/{}/results/farm_' strrep(analysis_struct.task.farm_name, ' ', '') '/block_' strrep(analysis_struct.task.block_name, ' ', '') '/' analysis_struct.task.session_name];

