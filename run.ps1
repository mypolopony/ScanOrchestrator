New-Item -ItemType Directory -Force -Path C:\Users\agridata\.aws
cp C:\AgriData\Projects\aws\credentials C:\Users\agridata\.aws\ 

New-Item -ItemType Directory -Force -Path C:\Users\agridata\.config 
New-Item -ItemType Directory -Force -Path C:\Users\agridata\.config\rclone 
cp C:\AgriData\Projects\.config\rclone\rclone.conf C:\Users\agridata\.config\rclone\ 

cp C:\AgriData\Projects\git\* C:\Users\agridata\ 

git -C C:\AgriData\Projects\ScanOrchestrator pull 
git -C C:\AgriData\Projects\MatlabCore pull 

pythonw C:\AgriData\Projects\ScanOrchestrator\orchestrator.py  
echo "Done!" 

exit