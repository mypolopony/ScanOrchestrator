#/bin/bash

# Provide the subscription Id where snapshot is created
subscriptionId=86655500-654c-4374-8618-faae891c053e

# Provide the name of your resource group where snapshot is created
resourceGroupName=preprocess6

# Provide the snapshot/disk name 
# snapshotName=computeos_snapshot
diskName=jumpbox_disk1_8e47aaf83fc24d0a9269ddca1963d02a

# Provide Shared Access Signature (SAS) expiry duration in seconds e.g. 3600.
# Know more about SAS here: https://docs.microsoft.com/en-us/azure/storage/storage-dotnet-shared-access-signature-part-1
sasExpiryDuration=3600

# Provide storage account name where you want to copy the snapshot. 
storageAccountName=agridatacomputestorage

# Name of the storage container where the downloaded snapshot will be stored
storageContainerName=images

# Provide the key of the storage account where you want to copy snapshot. 
storageAccountKey=HTqbKnZ/9/lo+G1ohr+KUYteOZSsrh4yqkgIc0KwlaXc+lx6egTVsG2qHCYDm5B4efTT0KX+wk+bNZmZmgrNfA==

# Provide the name of the VHD file to which snapshot will be copied.
destinationVHDFileName=computeos.vhd

# Set the subscription id
az account set --subscription $subscriptionId

# Generate and obtain the the uri
# sas=$(az snapshot grant-access --resource-group $resourceGroupName --name $snapshotName --duration-in-seconds $sasExpiryDuration -o tsv)
sas=$(az disk grant-access --resource-group $resourceGroupName --name $diskName --duration-in-seconds $sasExpiryDuration -o tsv)

# Start the copy
az storage blob copy start --destination-blob $destinationVHDFileName --destination-container $storageContainerName --account-name $storageAccountName --account-key $storageAccountKey --source-uri $sas

# Monitor the copy
watch --color -n 0.5 "az storage blob show -c $storageContainerName --account-name $storageAccountName --account-key $storageAccountKey -n $destinationVHDFileName"