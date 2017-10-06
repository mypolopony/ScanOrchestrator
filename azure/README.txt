cd ScanOrchestrator/azure/

Please makesure that run.sh is uploaded to ,
https://agridatacomputestorage.blob.core.windows.net/scripts/run.sh  --for westus2
https://kggamowstorage.blob.core.windows.net/scriptslinux/run.sh     --for eastus
and run.ps1 is uploaded to
https://agridatacomputestorage.blob.core.windows.net/scripts/run.ps1
https://kggamowstorage.blob.core.windows.net/scriptswin/run.ps1

Usage for creating a set of resources for Windows vmss in westus2 with RG==boo:
cd ScanOrchestrator/azure
./deploy_common.sh -c -l westus2 -r boo -o Windows


Usage for deleting an esisting set of resources for Linux vmss in eastus with RG==boo:
cd ScanOrchestrator/azure
./deploy_common.sh -d -l eastus -r boo -o Linux

Suggestion use simple names of length less than 4 letters for resource groups, as the names of the resources created are dervied from,
tye RG name. Convention that kgeorge uses, for windows vmss RG , 'KSCnt', K is a mnemonic, SC stands for south central, nt stands for windows






