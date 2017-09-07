#!/bin/bash

# Task Control
# set -m

# machinetypes=( gates torvalds )

# for type in "${machinetypes[@]}"
# do
#     echo $i

# Windows
az vmss stop -g symphony -n gates
az vmss delete-instances -n gates --instance-ids '*' -g symphony
az vmss scale -g symphony -n gates --new-capacity 32
az vmss start -g symphony -n gates

# Linux
az vmss stop -g symphony -n torvalds
az vmss delete-instances -n torvalds --instance-ids '*' -g symphony
az vmss scale -g symphony -n torvalds --new-capacity 12
az vmss start -g symphony -n torvalds



# done

# /usr/bin/python /Users/mypolopony/Projects/ScanOrchestrator/initiate.py