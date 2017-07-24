New-Item -ItemType Directory -Force -Path ~\.aws > ~/status.log
cp C:\AgriData\Projects\aws\credentials ~\.aws\ >> ~/status.log

New-Item -ItemType Directory -Force -Path ~\.config >> ~/status.log
New-Item -ItemType Directory -Force -Path ~\.config\rclone >> ~/status.log
cp C:\AgriData\Projects\.config\rclone\rclone.conf C:\Users\agridata\.config\rclone\ >> ~/status.log

cp C:\AgriData\Projects\git\* ~\  >> ~/status.log

git -C C:\AgriData\Projects\ScanOrchestrator pull >> ~/status.log
git -C C:\AgriData\Projects\MatlabCore pull >> ~/status.log

pythonw C:\AgriData\Projects\ScanOrchestrator\orchestrator.py  >> ~/status.log 
echo "Done!" >> ~/status.log

exit