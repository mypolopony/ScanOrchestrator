param(
    [string]$scanOrchestratorBranch="everett-wheeler",
    [string]$matlabCoreBranch="master",
    [string]$session_name="trash"
)


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
[Environment]::SetEnvironmentVariable("AWS_ACCESS_KEY_ID", "AKIAIQYWKQQF5NKCCPGA", "Machine")
[Environment]::SetEnvironmentVariable("AWS_SECRET_ACCESS_KEY", "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", "Machine")


## Pip Dependencies (bake into future images)
pip install kombu
pip install psutil

### Update ScanOrchestrator
echo "$(Get-Date): Updating ScanOrchestrator $scanOrchestratorBranch" >> C:\Users\agridata\startup.log
cd C:\AgriData\Projects\ScanOrchestrator
git remote rm origin
git remote add origin "https://mypolopony:Waffles2003@github.com/motioniq/ScanOrchestrator.git"
git gc --prune=now
git fetch --all
#assuming temp branch doesnt exist
$temp_branch= -join("temp_", $scanOrchestratorBranch)
git checkout -b $temp_branch
git reset --hard origin/$scanOrchestratorBranch 


### Update MatlabCore
echo "$(Get-Date): Updating MatlabCore $matlabCoreBranch" >> C:\Users\agridata\startup.log
cd C:\AgriData\Projects\MatlabCore
git remote rm origin
git remote add origin "https://mypolopony:Waffles2003@github.com/motioniq/MatlabCore.git"
git gc --prune=now
git fetch --all
#assuming temp branch doesnt exist
$temp_branch= -join("temp_", $matlabCoreBranch)
git checkout -b $temp_branch
git reset --hard origin/$matlabCoreBranch


### Copy extern
echo "$(Get-Date): Copy extern" >> C:\Users\agridata\startup.log
aws s3 cp s3://agridataselwyn/extern C:\AgriData\Projects\MatlabCore\extern\ --recursive
cp C:\AgriData\Projects\MatlabCore\extern\vlfeat-0.9.20\bin\win64\vcomp100.dll C:\Windows\System32


###Copy session_name to C:\AgriData\Projects\ScanOrchestrator\data\overriding.conf
$tgt_file=C:\AgriData\Projects\ScanOrchestrator\data\overriding.conf
echo "Writing session_name:$session_name to $tgt_file" >> C:\Users\agridata\startup.log
New-Item -ItemType Directory -Force -Path (Split-Path -parent $tgt_file)
echo "[args]`nsession_name=$session_name`n" >  $tgt_file

### Launch Orchestrator
echo "$(Get-Date): Launching Orchestrator" >> C:\Users\agridata\startup.log
pythonw C:\AgriData\Projects\ScanOrchestrator\orchestrator.py


### Signoff
echo "$(Get-Date): Finished" >> C:\Users\agridata\startup.log

exit
