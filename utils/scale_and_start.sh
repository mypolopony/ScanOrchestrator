#!/bin/bash

# Task Control
# set -m

# machinetypes=( gates torvalds )

# for type in "${machinetypes[@]}"
# do
#     echo $i

# Linux
az vmss stop -g symphony -n torvalds
az vmss delete-instances -n torvalds --instance-ids '*' -g symphony
az vmss scale -g symphony -n torvalds --new-capacity 2
az vmss start -g symphony -n torvalds

# Windows
az vmss stop -g symphony -n gates
az vmss delete-instances -n gates --instance-ids '*' -g symphony
az vmss scale -g symphony -n gates --new-capacity 4
az vmss start -g symphony -n gates

# done

# /usr/bin/python /Users/mypolopony/Projects/ScanOrchestrator/initiate.py