param(
    [string]$scanOrchestratorBranch="everett-wheeler",
    [string]$matlabCoreBranch="master",
    [string]$session_name="trash"
)

### Signon
echo "$(Get-Date): Starting" > C:\Users\agridata\startup.log
echo $env:username >> C:\Users\agridata\startup.log


### Set environment variables and try to give system AWS credentials, too
New-Item -ItemType Directory -Force -Path ~\.aws
echo "[default]" > ~\.aws\credentials
echo "aws_access_key_id = AKIAIQYWKQQF5NKCCPGA" >> ~\.aws\credentials
echo "aws_secret_access_key = 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" >> ~\.aws\credentials


## Pip Dependencies (bake into future images)
pip install kombu
pip install psutil

### Update ScanOrchestrator
echo "$(Get-Date): Updating ScanOrchestrator" >> C:\Users\agridata\startup.log
cd C:\AgriData\Projects\ScanOrchestrator
git remote rm origin
git remote add origin "https://mypolopony:Waffles2003@github.com/motioniq/ScanOrchestrator.git"
git fetch --all
git reset --hard origin/master
git checkout master


### Update MatlabCore
echo "$(Get-Date): Updating MatlabCore" >> C:\Users\agridata\startup.log
cd C:\AgriData\Projects\MatlabCore
git remote rm origin
git remote add origin "https://mypolopony:Waffles2003@github.com/motioniq/MatlabCore.git"
git fetch --all
git reset --hard origin/master
git checkout master


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