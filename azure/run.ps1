### Signon
echo "$(Get-Date): Starting" > C:\Users\agridata\startup.log
echo "$(Get-Date): Starting (again)" >> C:\Users\agridata\startup.log

### Signon
echo "$(Get-Date): Starting" > C:\Users\agridata\startup_again.log
echo "$(Get-Date): Starting (again)" > C:\Users\agridata\startup_again.log


### AWS / Rclone credentials
# $env:AWS_ACCESS_KEY_ID = "AKIAJCTBYUIK37F3OUYA"
# $env:AWS_SECRET_ACCESS_KEY = "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"

New-Item -ItemType Directory -Force -Path C:\Users\agridata\.aws
cp C:\AgriData\Projects\aws\credentials C:\Users\agridata\.aws\ 
New-Item -ItemType Directory -Force -Path C:\Users\agridata\.config 
New-Item -ItemType Directory -Force -Path C:\Users\agridata\.config\rclone 
cp C:\AgriData\Projects\.config\rclone\rclone.conf C:\Users\agridata\.config\rclone\ 


### Update ScanOrchestrator
echo "$(Get-Date): Updating ScanOrchestrator" >> C:\Users\agridata\startup.log
cd C:\AgriData\Projects\ScanOrchestrator
git remote rm origin
git remote add origin "https://mypolopony:Waffles2003@github.com/motioniq/ScanOrchestrator.git"
git fetch --all
git reset --hard origin/everett-wheeler 


### Update MatlabCore
echo "$(Get-Date): Updating MatlabCore" >> C:\Users\agridata\startup.log
cd C:\AgriData\Projects\MatlabCore
git remote rm origin
git remote add origin "https://mypolopony:Waffles2003@github.com/motioniq/MatlabCore.git"
git fetch --all
git reset --hard origin/master


### Launch Orchestrator
echo "$(Get-Date): Launching Orchestrator" >> C:\Users\agridata\startup.log
pythonw C:\AgriData\Projects\ScanOrchestrator\orchestrator.py


### Signoff
echo "$(Get-Date): Finished" >> C:\Users\agridata\startup.log