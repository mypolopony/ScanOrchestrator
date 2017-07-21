mkdir ~\.aws
cp E:\Projects\aws\credentials ~\.aws\

mkdir ~\.config
mkdir ~\.config\rclone
cp E:\Projects\.config\rclone\rclone.conf ~\.config\

cp E:\Projects\git\* ~\

git -C E:\Projects\ScanOrchestrator pull
git -C E:\Projects\MatlabCore pull

# python E:\Projects\ScanOrchestrator\orchestrator.py