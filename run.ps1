New-Item -ItemType Directory -Force -Path C:\Users\agridata\.aws
cp C:\AgriData\Projects\aws\credentials C:\Users\agridata\.aws\ 

Get-Date > C:\Users\agridata\date.txt

New-Item -ItemType Directory -Force -Path C:\Users\agridata\.config 
New-Item -ItemType Directory -Force -Path C:\Users\agridata\.config\rclone 
cp C:\AgriData\Projects\.config\rclone\rclone.conf C:\Users\agridata\.config\rclone\ 

git config --global credential.helper 'store --file C:\AgriData\git\.git-credentials'
# cp C:\AgriData\Projects\git\* C:\Users\agridata\ 

cd C:\AgriData\Projects\ScanOrchestrator
git fetch --all
git reset --hard origin/everett-wheeler

cd C:\AgriData\Projects\MatlabCore
git fetch --all
git reset --hard origin/master 

pythonw C:\AgriData\Projects\ScanOrchestrator\orchestrator.py

exit

Get-Date > C:\Users\agridata\date-finished.txt