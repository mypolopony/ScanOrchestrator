import matlab.engine

startpath=r'C:\AgriData\Projects'

task = {"farmid": "5994c6ab55f30b158613c517", "farm_name": "Quatacker-Burns", "client_name": "Sun World", "blockid": "599f7ac855f30b2756ca2b5a", "clientid": "591daa81e1cf4d8cbfbb1bf6", "block_name": "3C", "role": "shapesize", "detection_params": {"caffemodel_s3_url_cluster": "s3://deeplearning_data/models/best/cluster_june_15_288000.caffemodel", "caffemodel_s3_url_trunk": "s3://deeplearning_data/models/best/trunk_june_10_400000.caffemodel", "folders":['2017-08-17_13-59_22005520_14_15-preprocess-row97-dir1-1of2.zip', '2017-08-17_13-59_22005520_14_06-preprocess-row89-dir2-1of2.zip', '2017-08-17_13-59_22005520_14_18-preprocess-row99-dir1-2of2.zip', '2017-08-17_13-59_22005516_14_05-preprocess-row88-dir2-1of2.zip', '2017-08-17_13-59_22005520_14_21-preprocess-row101-dir2-2of2.zip', '2017-08-17_13-59_22005516_14_15-preprocess-row96-dir2-2of3.zip', '2017-08-17_13-59_22005516_14_21-preprocess-row102-dir1-2of2.zip', '2017-08-17_13-59_22005516_14_00-preprocess-row86-dir1-1of3.zip', '2017-08-17_13-59_22005516_14_09-preprocess-row90-dir2-3of3.zip', '2017-08-17_13-59_22005520_14_10-preprocess-row93-dir1-1of2.zip', '2017-08-17_13-59_22005520_14_00-preprocess-row85-dir1-2of2.zip', '2017-08-17_13-59_22005520_14_03-preprocess-row87-dir1-2of2.zip', '2017-08-17_13-59_22005516_14_11-preprocess-row92-dir2-2of2.zip', '2017-08-17_13-59_22005516_14_19-preprocess-row100-dir1-2of2.zip']}, "test": 0, "scanids": ["2017-09-14_08-09"], "session_name": "SW_qt_3c_913_wed_1PM"}


mlab = matlab.engine.start_matlab()
mlab.addpath(mlab.genpath(startpath))
mlab.runTask(task, nargout=0)