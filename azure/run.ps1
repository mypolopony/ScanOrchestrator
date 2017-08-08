### Signon
echo "$(Get-Date): Starting" > C:\Users\agridata\startup.log
echo $env:username >> C:\Users\agridata\startup.log


### AWS / Rclone credentials
New-Item -ItemType Directory -Force -Path C:\Users\agridata\.aws
cp C:\AgriData\Projects\aws\credentials C:\Users\agridata\.aws\ 
New-Item -ItemType Directory -Force -Path C:\Users\agridata\.config 
New-Item -ItemType Directory -Force -Path C:\Users\agridata\.config\rclone 
cp C:\AgriData\Projects\.config\rclone\rclone.conf C:\Users\agridata\.config\rclone\ 


### Set environment variables and try to give system AWS credentials, too
New-Item -ItemType Directory -Force -Path ~\.aws
cp C:\AgriData\Projects\aws\credentials ~\.aws\ 
[Environment]::SetEnvironmentVariable("AWS_ACCESS_KEY_ID", "AKIAJCTBYUIK37F3OUYA", "Machine")
[Environment]::SetEnvironmentVariable("AWS_SECRET_ACCESS_KEY", "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", "Machine")

## Pip Dependencies (bake into future images)
pip install pika
pip install kombu

### Update ScanOrchestrator
echo "$(Get-Date): Updating ScanOrchestrator" >> C:\Users\agridata\startup.log
cd C:\AgriData\Projects\ScanOrchestrator
git remote rm origin
git remote add origin "https://mypolopony:Waffles2003@github.com/motioniq/ScanOrchestrator.git"
git gc --prune=now
git fetch --all
git reset --hard origin/queues


### Update MatlabCore
echo "$(Get-Date): Updating MatlabCore" >> C:\Users\agridata\startup.log
cd C:\AgriData\Projects\MatlabCore
git remote rm origin
git remote add origin "https://mypolopony:Waffles2003@github.com/motioniq/MatlabCore.git"
git gc --prune=now
git fetch --all
git reset --hard origin/master


### Copy extern
echo "$(Get-Date): Copy extern" >> C:\Users\agridata\startup.log
aws s3 cp s3://agridataselwyn/extern C:\AgriData\Projects\MatlabCore\extern\ --recursive
cp C:\AgriData\Projects\MatlabCore\extern\vlfeat-0.9.20\bin\win64\vcomp100.dll C:\Windows\System32


### Launch Orchestrator
echo "$(Get-Date): Launching Orchestrator" >> C:\Users\agridata\startup.log
pythonw C:\AgriData\Projects\ScanOrchestrator\orchestrator.py


### Signoff
echo "$(Get-Date): Finished" >> C:\Users\agridata\startup.log

exit