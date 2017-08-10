cd ScanOrchestrator/azure/linux

Please makesure that run.sh is uploaded to https://agridatacomputestorage.blob.core.windows.net/scriptslinux/run.sh

Please change the variable 'RG' in deploy.sh to the resource group name that you want

#to create deployment please do
./deploy.sh -c 1 -d 0 -l <location>
<location> = westus2, southcentralus, eastus
#to delete what was created please do
./deploy.sh -c 0 -d 1 -l <location>

