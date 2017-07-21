mkdir ~\.aws
cp C:\AgriData\Projects\aws\credentials ~\.aws\

New-Item -ItemType Directory -Force -Path ~\.config
New-Item -ItemType Directory -Force -Path ~\.config\rclone
cp C:\AgriData\Projects\.config\rclone\rclone.conf ~\.config\

cp C:\AgriData\Projects\git\* ~\

git -C C:\AgriData\Projects\ScanOrchestrator pull
git -C C:\AgriData\Projects\MatlabCore pull

python C:\AgriData\Projects\ScanOrchestrator\orchestrator.py