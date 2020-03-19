param ( 
    [parameter(Mandatory=$true)] [ValidateSet("dev", "tst", "stg","prd")] [string] $env,
    [parameter(Mandatory=$true)] [string] $resourceGroupName, 
    [parameter(Mandatory=$true)] [string] $subscriptionId, 
    [parameter(Mandatory=$true)] [string] $environment,
    [parameter(Mandatory=$true)] [string] $region,
    [parameter(Mandatory=$true)] [string] $pathToJson

)
function Get-Region-Shorthand () {   
    param (
        [parameter(Mandatory=$true)] [string] $region = "East US 2" 
    )
    #List of Regions need to be validated by Mike Martin for the shorthands
    $regions = @{
        'East US' = 'eus1'
        'East US 1' = 'eus1'
        'East US 2' = 'eus2'
        'North Central US 1' = 'ncus'
        'South West US' = 'swus'
        'West US 1' = 'wus1'
        'West US 2' = 'wus2'
        'North Europe' = 'neur'
        'West Europe' = 'weur'
        'UK South' = 'uks'
        'UK West' = 'ukw'
        'Canada Central' = 'canc'
        'Canada East' = 'cane'
    }
    $shorthand = $regions[$region];
    if (-Not $shorthand) {
        throw "[Get-Region-Shorthand] $region not recognized"
    }
    return $shorthand
}

$thisScript = Split-Path -Path $MyInvocation.MyCommand.Definition -Parent
$shortRegion = Get-Region-Shorthand -region $region
$NewParameterFile = "$thisScript\newARMTemplateParametersForFactory.json" 
$jsonContent =  Get-Content $pathToJson 
#$jsonContent = $jsonContent -replace '/resourceGroups/\w+-\w+-\w+-\w+-\w+/', "/resourceGroups/$resourceGroupName/"
$jsonContent = $jsonContent -replace '/resourceGroups/\w+-\w+-\w+', "/resourceGroups/azu-$shortRegion-$environment"
$jsonContent = $jsonContent -replace '/storageAccounts/azu\w{7}', "/storageAccounts/azu$shortRegion$environment"
$jsonContent = $jsonContent -replace '/subscriptions/\w{8}-\w{4}-\w{4}-\w{4}-\w{12}/', "/subscriptions/$subscriptionId/"
$jsonContent |set-content $NewParameterFile

