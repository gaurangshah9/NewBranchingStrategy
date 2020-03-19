# Needs the CMDLet from https://github.com/PlagueHO/CosmosDB
# Install-Module -Name CosmosDB
<#
    ./Create-Cosmos-Collections.ps1 -resourceGroupName azu-eus2-dev-rg-DPCoordinator -CosmosDbAccountName azu-eus2-dev-cdb-datalakecoord `
       -cosmosDBName coordination -collections "coordination_configuration,events_datavault_complete,events_informationmart_complete"

    ##Requires -Modules CosmosDB
#>
param (
    [parameter(Mandatory=$true)] [string] $ResourceGroupName ="azu-eus2-dev-rg-DPCoordinator",
    [parameter(Mandatory=$true)] [string] $CosmosDbAccountName = "azu-eus2-dev-cdb-datalakecoord",
    [parameter(Mandatory=$true)] [string] $cosmosDBName = "coordination",
    [parameter(Mandatory=$true)] [string] $collections = "configuration,linked_services,dataset_events,activities",
    [parameter(Mandatory=$false)] [int] $throughput = 400
)

if (-Not (Get-Module -ListAvailable -Name CosmosDB)) {
    Install-Module -Name CosmosDB -Force -Scope CurrentUser -allowclobber 
}
#Remove-Module -name AzureRM -Force
#Uninstall-Module -name AzureRM.Profile -Force -Scope CurrentUser
Import-Module Az.Accounts -Force -Scope CurrentUser
Enable-AzureRmAlias -Scope CurrentUser
Install-Module -Name CosmosDB -Force -Scope CurrentUser -allowclobber 
Import-Module -Name CosmosDB -Force -Scope CurrentUser
Get-InstalledModule -name CosmosDB

# Get a reference to the Cosmos Account
Write-host "Getting Context"
$ctx = New-CosmosDbContext -Account  $CosmosDbAccountName -ResourceGroup $ResourceGroupName -MasterKeyType 'PrimaryMasterKey'

# Create Cosmos Database

try {
    Write-Host "Checking if Database $cosmosDBName Exists"
    $cdb = Get-CosmosDbDatabase -context $ctx -id $cosmosDBName
    Write-host "true"
} catch {
    Write-Host "Creating database $cosmosDBName"
    New-CosmosDbDatabase -Context $ctx -Id $cosmosDBName
}

#Create Collections
$collectionArray = $collections.Split(',')
foreach ($c in $collectionArray) {
    try {
        Write-Host "Checking if Collection $c Exists"
        $col = get-CosmosDbCollection -Context $ctx -Database $cosmosDBName -Id $c
        Write-host "true"
    }
    catch {
        Write-Host "Creating collection $c"
        New-CosmosDbCollection -Context $ctx -Database $cosmosDBName -OfferThroughput $throughput -id $c
    }
}