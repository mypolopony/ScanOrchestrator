#!/bin/bash

echo "eastus" && az vm list-usage -l eastus -o table | grep NC
echo "eastus2" && az vm list-usage -l eastus2 -o table | grep NC
echo "centralus" && az vm list-usage -l centralus -o table | grep NC
echo "northcentralus" && az vm list-usage -l northcentralus -o table | grep NC
echo "southcentralus" && az vm list-usage -l southcentralus -o table | grep NC
echo "westcentralus" && az vm list-usage -l westcentralus -o table | grep NC
echo "westus" && az vm list-usage -l westus -o table | grep NC
echo "westus2" && az vm list-usage -l westus2 -o table | grep NC
echo "canadaeast" && az vm list-usage -l canadaeast -o table | grep NC
echo "canadacentral" && az vm list-usage -l canadacentral -o table | grep NC