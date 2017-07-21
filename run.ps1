echo "hi" > C:\doinggood
whoami >> C:\doinggood

New-Item -ItemType Directory -Force -Path ~\.aws
cp C:\AgriData\Projects\aws\credentials ~\.aws\

New-Item -ItemType Directory -Force -Path ~\.config
New-Item -ItemType Directory -Force -Path ~\.config\rclone
cp C:\AgriData\Projects\.config\rclone\rclone.conf C:\AgriData\.config\rclone\

cp C:\AgriData\Projects\git\* ~\

git -C C:\AgriData\Projects\ScanOrchestrator pull >> ~/help
git -C C:\AgriData\Projects\MatlabCore pull >> ~/help

# python C:\AgriData\Projects\ScanOrchestrator\orchestrator.py >> ~/help