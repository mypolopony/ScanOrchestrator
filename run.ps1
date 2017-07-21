mkdir ~\.aws
cp C:\AgriData\Projects\aws\credentials ~\.aws\ > ~/help

New-Item -ItemType Directory -Force -Path ~\.config >> ~/help
New-Item -ItemType Directory -Force -Path ~\.config\rclone >> ~/help
cp C:\AgriData\Projects\.config\rclone\rclone.conf ~\.config\ >> ~/help

cp C:\AgriData\Projects\git\* ~\ >> ~/help

git -C C:\AgriData\Projects\ScanOrchestrator pull >> ~/help
git -C C:\AgriData\Projects\MatlabCore pull >> ~/help

python C:\AgriData\Projects\ScanOrchestrator\orchestrator.py >> ~/help