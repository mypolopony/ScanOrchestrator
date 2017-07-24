New-Item -ItemType Directory -Force -Path C:\Users\agridata\.aws > C:\Users\agridata\status.log
cp C:\AgriData\Projects\aws\credentialsC:\Users\agridata\.aws\ >> C:\Users\agridata\status.log

New-Item -ItemType Directory -Force -Path C:\Users\agridata\.config >> C:\Users\agridata\status.log
New-Item -ItemType Directory -Force -Path C:\Users\agridata\.config\rclone >> C:\Users\agridata\status.log
cp C:\AgriData\Projects\.config\rclone\rclone.conf C:\Users\agridata\.config\rclone\ >> C:\Users\agridata\status.log

cp C:\AgriData\Projects\git\* C:\Users\agridata\ >> C:\Users\agridata\status.log

git -C C:\AgriData\Projects\ScanOrchestrator pull >> C:\Users\agridata\status.log
git -C C:\AgriData\Projects\MatlabCore pull >> C:\Users\agridata\status.log

pythonw C:\AgriData\Projects\ScanOrchestrator\orchestrator.py  >> C:\Users\agridata\status.log 
echo "Done!" >> C:\Users\agridata\status.log

exit