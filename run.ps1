mkdir ~\.aws
cp E:\Projects\aws\credentials ~\.aws\

mkdir ~\.config
mkdir ~\.config\rclone
cp E:\Projects\.config\rclone\rclone.conf ~\.config\

cp E:\Projects\git\* ~\

git pull -C E:\Projects\ScanOrchestrator
git pull -C E:\Projects\MatlabCore

# python E:\Projects\ScanOrchestrator\orchestrator.py