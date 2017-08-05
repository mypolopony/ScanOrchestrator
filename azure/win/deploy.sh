#!/bin/bash


DELETE=0
CREATE=1
LOC=eastus
while getopts d:c:p: opts; do
   case ${opts} in
      d) DELETE=${OPTARG} ;;
      c) CREATE=${OPTARG} ;;
      l) LOC=${OPTARG} ;;
   esac
done


echo DELETE=$DELETE  CREATE=$CREATE LOC=$LOC



KEY=processE
RG=processE
OUTDIR=./output
DEPLOYMENT_NAME=$KEY-depl
mkdir -p $OUTDIR
SRC_DIR=.

if [[ -z KEPLONG ]]; then
    echo ivide sukhamaaNu
fi

if [ "$CREATE" -eq "1" ];then
    paramsJson=$(cat  $SRC_DIR/vmssdeploy.parameters.json | jq '.parameters')
    echo creating resource group
    az group create -n $RG   -l $LOC > $OUTDIR/$RG-output.json
    echo creating storage account $STORAGE


    templateFile=$SRC_DIR/vmssdeploy.json
    paramsJson=$(cat  $SRC_DIR/vmssdeploy.parameters.json | jq '.parameters')
    #paramsJson=$( echo "$paramsJson"  | jq "{_artifactsLocation: {value: "\"$blobEndpoint$STORAGE_CONT"\"}, _artifactsLocationSasToken: {value: \"?"$sasToken"\"}} + ." )
    echo $paramsJson

    #az group deployment validate -g $RG   --template-file $templateFile --parameters "$paramsJson" --verbose

    az group deployment create -g $RG  -n $DEPLOYMENT_NAME  --template-file $templateFile --parameters "$paramsJson" --verbose >  $OUTDIR/$DEPLOYMENT_NAME-output.json


fi





if [ "$DELETE" -eq "1" ];then
    echo deleting resources
    az group deployment delete -g $RG  -n $DEPLOYMENT_NAME
    az group delete -n $RG
fi

