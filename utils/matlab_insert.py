import matlab.engine

startpath=r'C:\AgriData\Projects'

task = {"farmid": "5994c6ab55f30b158613c517", "farm_name": "Quatacker-Burns", "client_name": "Sun World", "blockid": "599f7ac855f30b2756ca2b5a", "clientid": "591daa81e1cf4d8cbfbb1bf6", "block_name": "3C", "role": "shapesize", "detection_params": {"caffemodel_s3_url_cluster": "s3://deeplearning_data/models/best/cluster_june_15_288000.caffemodel", "caffemodel_s3_url_trunk": "s3://deeplearning_data/models/best/trunk_june_10_400000.caffemodel"}, "test": 0, "scanids": ["2017-09-14_08-09"], "session_name": "3c"}

mlab = matlab.engine.start_matlab()
mlab.addpath(mlab.genpath(startpath))
mlab.runTask(task, nargout=0)
