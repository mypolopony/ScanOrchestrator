#!/bin/bash



DELETE=1
CREATE=1
LOC=westus2
while getopts d:c:l: opts; do
   case ${opts} in
      d) DELETE=${OPTARG} ;;
      c) CREATE=${OPTARG} ;;
      l) LOC=${OPTARG} ;;
   esac
done


echo DELETE=$DELETE  CREATE=$CREATE LOC=$LOC



RG="torvalds"
OUTDIR=./output
DEPLOYMENT_NAME=$RG
mkdir -p $OUTDIR
SRC_DIR=.

if [[ -z KEPLONG ]]; then
    echo ivide sukhamaaNu
fi

if [ "$DELETE" -eq "1" ];then
    echo deleting resources
    echo az group deployment delete -g $RG  -n $DEPLOYMENT_NAME
    az group deployment delete -g $RG  -n $DEPLOYMENT_NAME
    # Considering using the --yes flag here to automatically confirm
    echo az group delete -n $RG
    az group delete -n $RG
fi

if [ "$CREATE" -eq "1" ];then
    paramsJson=$(cat  $SRC_DIR/vmssdeploy_$LOC.parameters.json | jq '.parameters')
    echo az group create -n $RG   -l $LOC > $OUTDIR/$RG-output.json
    az group create -n $RG   -l $LOC > $OUTDIR/$RG-output.json


    templateFile=$SRC_DIR/vmssdeploy.json
    paramsJson=$(cat  $SRC_DIR/vmssdeploy_$LOC.parameters.json | jq '.parameters')
    echo $paramsJson

    #az group deployment validate -g $RG   --template-file $templateFile --parameters "$paramsJson" --verbose
    echo az group deployment create -g $RG  -n $DEPLOYMENT_NAME  --template-file $templateFile --parameters "$paramsJson" --verbose >  $OUTDIR/$DEPLOYMENT_NAME-output.json
    az group deployment create -g $RG  -n $DEPLOYMENT_NAME  --template-file $templateFile --parameters "$paramsJson" --verbose >  $OUTDIR/$DEPLOYMENT_NAME-output.json

fi
