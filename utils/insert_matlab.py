import matlab.engine

startpath=r'C:\AgriData\Projects'

{"farmid": "58f68e511fb3595cb3e21620", "farm_name": "Slawson", "client_name": "Harlan Estate", "blockid": "58f68e511fb3595cb3e21625", "clientid": "58f5e3c21fb35955235c7b31", "block_name": "A3", "role": "postprocess", "detection_params": {"caffemodel_s3_url_cluster": "s3://deeplearning_data/models/best/wine-black-green-sep-19-2017-100000.caffemodel", "caffemodel_s3_url_trunk": "s3://deeplearning_data/models/best/trunk_june_10_400000.caffemodel"}, "test": 0, "scanids": ["2017-08-23_08-51", "2017-08-23_09-14"], "session_name": "a3"}

mlab = matlab.engine.start_matlab()
mlab.addpath(mlab.genpath(startpath))
mlab.runTask(task, nargout=0)