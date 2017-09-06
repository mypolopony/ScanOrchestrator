#!/bin/bash

# Linux
az vmss stop -g symphony -n torvalds
az vmss scale -g symphony -n torvalds --new-capacity 18
az vmss start -g symphony -n torvalds --instance-ids '*'

# Windows
az vmss stop -g symphony -n gates
az vmss scale -g symphony -n gates --new-capacity 24
az vmss start -g symphony -n gates --instance-ids '*'

/usr/bin/python /Users/mypolopony/Projects/ScanOrchestrator/initiate.py