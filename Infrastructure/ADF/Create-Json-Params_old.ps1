param ( 
    [parameter(Mandatory=$true)] [ValidateSet("dev", "tst", "stg","prd")] [string] $env,
    [parameter(Mandatory=$true)] [string] $resourceGroupName, 
    [parameter(Mandatory=$true)] [string] $subscriptionId, 
    [parameter(Mandatory=$true)] [string] $storageAccountName,
    [parameter(Mandatory=$true)] [string] $pathToJson

)
$thisScript = Split-Path -Path $MyInvocation.MyCommand.Definition -Parent
$NewParameterFile = "$thisScript\newARMTemplateParametersForFactory.json" 
$jsonContent =  Get-Content $pathToJson 
$jsonContent = $jsonContent -replace '/resourceGroups/\w+-\w+-\w+-\w+-\w+/', "/resourceGroups/$resourceGroupName/"
$jsonContent = $jsonContent -replace '/storageAccounts/\w+', "/storageAccounts/$storageAccountName"
$jsonContent = $jsonContent -replace '/subscriptions/\w{8}-\w{4}-\w{4}-\w{4}-\w{12}/', "/subscriptions/$subscriptionId/"
$jsonContent |set-content $NewParameterFile