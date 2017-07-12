<#
    .SYNOPSIS
        Initiates the correct processing step upon startup
#>

python S:\Projects\ScanOrchestrator\orchestrator.py


Set-AzureRmVMCustomScriptExtension -ResourceGroupName compute `
    -VMName base-selwyn `
    -Location 'US WEST 2' `
    -Run 'python S:\Projects\ScanOrchestrator\orchestrator.py' `
    -Name StartOrchestrator