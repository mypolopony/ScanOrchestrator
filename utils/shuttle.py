import pickle
import os
from kombu import Connection
from datetime import datetime

def receivefromRabbitMQ(queue, num=1):
    msgs = list()
    with Connection('amqp://{}:{}@{}:5672//'.format('agridata','agridata','boringmachine')) as kbu:
        q = kbu.SimpleQueue(queue)
        while (len(msgs) < num and q.qsize() > 0):
            message = q.get(block=True)
            message.ack()
            msgs.append(message.payload)
        q.close()

        # If only one task is requested, release it from the list
        if num == 1:
            return msgs[0]
        # Otherwise, return a list of tasks
        else:
            return msgs


def sendtoRabbitMQ(queue, message):
    with Connection('amqp://{}:{}@{}:5672//'.format('agridata','agridata','boringmachine')) as kbu:
        q = kbu.SimpleQueue(queue)
        q.put(message)
        q.close()


outdir = '/Users/mypolopony/AgriData/picklejar'
target = 'detection_coronanorth'
replace = True
tag = datetime.strftime(datetime.now(), '%D-%T').replace('/','').replace(':','')
max_num = 999

messages = receivefromRabbitMQ(target, max_num)
with open(os.path.join(outdir, target + '-' + tag + '.dat'), 'wb') as dump:
    pickle.dump(messages, dump)

if replace:
    for msg in messages:
        sendtoRabbitMQ(target, msg)