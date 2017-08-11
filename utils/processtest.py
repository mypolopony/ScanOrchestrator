import multiprocess
import time
import matlab.engine
import psutil
import numpy as np

def launchMatlabTasks(task):
    '''
    A separate wrapper for multiple matlabs. It is called by multiprocess, which has the capability to
    pickle (actually, dill) a wider range of objects (like function wrappers)
    '''
    try:
        mlab = matlabProcess()
        mlab.processtest(task)
        mlab.quit()
    except Exception as e:
        print(e)
        pass

def matlabProcess(startpath=r'C:\AgriData\Projects'):
    '''
    Start MATLAB engine. This should not be global because it does not apply to all users of the script. Having said that,
    my hope is that it becomes a pain to pass around. The Windows path is a safe default that probably should be offloaded
    elsewhere since it
    '''
    print('Starting MATLAB. . .')
    mlab = matlab.engine.start_matlab()
    mlab.addpath(mlab.genpath(startpath))

    return mlab

def dummytask(n):
    print(dummytask)

def calculate(n):
    print('{}: Running with {}'.format(current_process,n))
    mlab = launchMatlabTasks(n)

    print('{}: Returning'.format(current_process))

if __name__ == '__main__': 
    subtasks = xrange(10)

    workers = list()
    for st in subtasks:
        worker = multiprocess.Process(target=launchMatlabTasks, args=[st])
        worker.start()
        workers.append(worker)

    matlabs = np.Inf            # So many matlabs
    while matlabs > 0:
        print('Waiting. . . {} MATLABS still alive'.format(matlabs))
        time.sleep(3)
        matlabs = len([p.pid for p in psutil.process_iter() if p.name().lower() == 'matlab.exe'])

    for worker in workers:
        worker.terminate()
    
    print('All MATLABS have finished')


        