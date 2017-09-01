### Signon
echo "$(Get-Date): Starting [ver. symphony]" > C:\Users\agridata\startup.log
echo $env:username >> C:\Users\agridata\startup.log


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
git checkout symphony


### Update MatlabCore
echo "$(Get-Date): Updating MatlabCore" >> C:\Users\agridata\startup.log
cd C:\AgriData\Projects\MatlabCore
git remote rm origin
git remote add origin "https://mypolopony:Waffles2003@github.com/motioniq/MatlabCore.git"
git fetch --all
git reset --hard origin/master
git checkout master


### AWS Credentials
New-Item -ItemType Directory -Force -Path ~\.aws
# Copy AWS credentials file from Scan Orchestrator
cp C:\AgriData\Projects\ScanOrchestrator\credentials\aws_credentials ~\.aws\credentials
# Also set the environment variables
[Environment]::SetEnvironmentVariable("AWS_ACCESS_KEY_ID", "AKIAIQYWKQQF5NKCCPGA", "Machine")
[Environment]::SetEnvironmentVariable("AWS_SECRET_ACCESS_KEY", "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", "Machine")

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