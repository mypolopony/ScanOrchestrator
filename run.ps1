echo "hi" > C:\doinggood
whoami >> C:\doinggood

New-Item -ItemType Directory -Force -Path ~\.aws >> C:\doinggood
cp C:\AgriData\Projects\aws\credentials ~\.aws\  >> C:\doinggood

New-Item -ItemType Directory -Force -Path ~\.config  >> C:\doinggood
New-Item -ItemType Directory -Force -Path ~\.config\rclone  >> C:\doinggood
cp C:\AgriData\Projects\.config\rclone\rclone.conf C:\AgriData\.config\rclone\  >> C:\doinggood

cp C:\AgriData\Projects\git\* ~\  >> C:\doinggood

git -C C:\AgriData\Projects\ScanOrchestrator pull >> C:\doinggood
git -C C:\AgriData\Projects\MatlabCore pull  >> C:\doinggood

pythonw C:\test.py >> C:\doinggood
pythonw C:\AgriData\Projects\ScanOrchestrator\orchestrator.py >> ~\help