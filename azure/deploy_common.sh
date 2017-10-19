#!/bin/bash

DELETE=1
CREATE=1
LOC=westus2
OSTYPE=Windows
ISDEBUG=0
RG=concerto
HELP=0
while getopts l:o:r:dcgh opts; do
   case ${opts} in
      d) DELETE=1 ;;
      c) CREATE=1 ;;
      l) LOC=${OPTARG} ;;
      o) OSTYPE=${OPTARG} ;;
      g) ISDEBUG=1;;
      r) RG=${OPTARG} ;;
      h) HELP=1;;
   esac
done

DATE=`date +%Y-%m-%d:%H:%M:%S`
echo ============================================================
echo starting $DATE



if [ "$HELP" == "1" ]
then
   echo "-d (flag, delete the specified resource, mutually exclusive with -c flag)"
   echo "-c (flag, create the specified resource, mutually exclusive with -d flag)"
   echo "-l (location should be eastus/southcentralus/westus2)"
   echo "-o (ostype, should be  Windows/Linux)"
   echo "-g (flag, debug mode)"
   echo "-r (name of resource group created)"
   echo "-h (flag, print help message and exit)"
   echo "Usage for creating a set of resources for Windows vmss in westus2 with RG==boo: cd ScanOrchestrator/azure; ./deploy_common.sh -c -l westus2 -r boo -o Windows"
   echo "Usage for deleting an esisting set of resources for Linux vmss in eastus with RG==boo: cd ScanOrchestrator/azure; ./deploy_common.sh -d -l eastus -r boo -o Linux"

   exit 0
fi

echo DELETE=$DELETE,  CREATE=$CREATE, LOC=$LOC, OSTYPE=$OSTYPE, ISDEBUG=$ISDEBUG, RG=$RG
#exit 0

RVM_SUFFIX=""

DEBUG=""
if [ "$ISDEBUG" -eq "1" ];then
    DEBUG="--debug"

fi


if [ "$OSTYPE" == "Linux" ]
then
    SRC_DIR=./linux

elif [ "$OSTYPE" == "Windows" ]
then
    SRC_DIR=./win
else
  echo "Error!" 1>&2
  exit -1
fi



OUTDIR=./output
mkdir -p $OUTDIR
DEPLOYMENT_NAME=$RG-depl
LOC2=$LOC
mkdir -p $OUTDIR

#    echo paramsJson | jq 'map(if .key == "instanceCount"
#          then . + {"value":"90"}
#          else .
#          end
#         )'



if [ "$DELETE" -eq "1" ];then

    echo deleting resources
    echo az group delete -n $RG
    az group delete -n $RG
fi


if [ "$CREATE" -eq "1" ];then
    DEPLOY_JSON_FILE=$SRC_DIR/vmssdeploy_${LOC2}${RVM_SUFFIX}.parameters.json
    templateFile=$SRC_DIR/vmssdeploy.json

    #then . + {"value":"{\"value\": 19}"}
    paramsJson=$(cat  ${DEPLOY_JSON_FILE} | jq '.parameters')

    echo creating resource group $RG, using template file $templateFile, and  reading parameters from $DEPLOY_JSON_FILE
    az group create -n $RG   -l $LOC > $OUTDIR/$RG-output.json


    #paramsJson=$(cat  ${DEPLOY_JSON_FILE} | jq '.parameters')
    echo content of $DEPLOY_JSON_FILE=$paramsJson

    #az group deployment validate -g $RG   --template-file $templateFile --parameters "$paramsJson" --verbose
    echo az group deployment create -g $RG  -n $DEPLOYMENT_NAME  --template-file $templateFile --parameters "$paramsJson" --verbose >  $OUTDIR/$DEPLOYMENT_NAME-output.json

    az group deployment create -g $RG  -n $DEPLOYMENT_NAME  --template-file $templateFile --parameters "$paramsJson" --verbose >  $OUTDIR/$DEPLOYMENT_NAME-output.json

fi


DATE=`date +%Y-%m-%d:%H:%M:%S`
echo ending $DATE
echo ============================================================
