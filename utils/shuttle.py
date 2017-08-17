import pickle
import os
from kombu import Connection
from datetime import datetime
import time

def receivefromRabbitMQ(queue):
    msgs = list()
    with Connection('amqp://{}:{}@{}:5672//'.format('agridata','agridata','boringmachine')) as kbu:
        q = kbu.SimpleQueue(queue)
        print(q.qsize())
        for i in xrange(q.qsize()):
            message = q.get()
            message.ack()
            msgs.append(message.payload)
        q.close()
    return msgs

def sendtoRabbitMQ(queue, message):
    with Connection('amqp://{}:{}@{}:5672//'.format('agridata','agridata','boringmachine')) as kbu:
        q = kbu.SimpleQueue(queue)
        q.put(message)
        q.close()


outdir = '/Users/mypolopony/AgriData/picklejar'
target = 'process_thpme10'
replace = True
copy = True
tag = datetime.strftime(datetime.now(), '%D-%T').replace('/','').replace(':','')
max_num = 999

datafile = os.path.join(outdir, target + '-' + tag + '.dat')

if copy:
    print('Saving messages from {}'.format(target))
    messages = receivefromRabbitMQ(target)
    with open(datafile, 'wb') as dump:
        pickle.dump(messages, dump)

if replace:
    print('Injecting messages from {}'.format(datafile))
    messages = pickle.load(open(datafile, 'rb'))
    for idx, msg in enumerate(messages):
        print('{}/{}'.format(idx,len(messages)))
        sendtoRabbitMQ(target, msg)