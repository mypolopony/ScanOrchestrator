#!/bin/bash

# Task Control
# set -m

# machinetypes=( gates torvalds )

# for type in "${machinetypes[@]}"
# do
#     echo $i

# Windows
az vmss stop -g ***RG*** -n gates
az vmss delete-instances -n gates --instance-ids '*' -g ***RG***
az vmss scale -g ***RG*** -n gates --new-capacity 32
az vmss start -g ***RG*** -n gates

# Linux
az vmss stop -g ***RG*** -n torvalds
az vmss delete-instances -n torvalds --instance-ids '*' -g ***RG***
az vmss scale -g ***RG*** -n torvalds --new-capacity 12
az vmss start -g ***RG*** -n torvalds



# done

# /usr/bin/python /Users/mypolopony/Projects/ScanOrchestrator/initiate.py