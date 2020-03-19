#Requires -Modules AzureRM.Storage
<#
    This script creates the top level folders in ADLS for Ingestion Retail Link, Alerts Generation and Team Alerts.
    Params:

        Env                     - Environment for the Data Lake Store. Values {dev|tst|stg|prd}
        dataLakeStoreName       - Data Lake Storage Gen1 account name.
        accessGroupName         - Group to be granted access in the ADLS. 
        topLevelFolder          - Script run mode. Values {ingestion|generation|team} 
        ListFile                - txt filename containing the client list. /Template

    Prerequisites:

    Author:
        Paulo Matioli
#>
param (
    [parameter(Mandatory=$true)] [ValidateSet("dev", "tst", "stg","prd")] [string] $env = "dev" ,
    [parameter(Mandatory=$true)] [string] $dataLakeStoreName = "azueus2devadlsdatalake",
    [string] $accessGroupName,
    [parameter(Mandatory=$true)] [string] $topLevelFolder,
    [parameter(Mandatory=$true)] [string] $ListFile,
    [string] $aclEntrybasePerm 
) 
$clientList = Get-Content -Path $ListFile
$accessGroupNameExist = [bool]$accessGroupName
$adminGroupId = (Get-AzureRmADGroup -SearchString "azu_sg_data_admin" |Select-Object -First 1).Id

try {
    $topLevelExist = Get-AzureRmDataLakeStoreChildItem -AccountName $dataLakeStoreName -Path $topLevelFolder -ErrorAction Stop
}
catch {
    write-host "Top level folder $topLevelFolder does not exist in $dataLakeStoreName ADLS storage."
    exit 8
}
if ($topLevelFolder -eq "/informationmart" -And $ListFile -like "*team_list.txt") {
    foreach ($client in $clientList){
        $client = $client.replace("-","_")
        $client = $client.toLower()
         # Create new INFORMATIONMART schema directory and apply permission
        try {
            $clientExist = Get-AzureRmDataLakeStoreChildItem -AccountName $dataLakeStoreName -Path "$topLevelFolder/${client}_team_alert/" -ErrorAction Stop
        }
        catch {
            $newFolder = New-AzureRmDataLakeStoreItem -AccountName $dataLakeStoreName -Folder "$topLevelFolder/${client}_team_alert/"
            write-host "New team INFORMATIONMART created at Storage Account $dataLakeStoreName path $newFolder"
        }
        Set-AzureRmDataLakeStoreItemOwner -Account $dataLakeStoreName -Path "$topLevelFolder/${client}_team_alert/" -Type Group -id $adminGroupId -WarningAction Ignore 
        if ($accessGroupNameExist) {
            $accessGroupId = (Get-AzureRmADGroup -SearchString $accessGroupName|Select-Object -First 1).Id
            Set-AzureRmDataLakeStoreItemAclEntry -Account $dataLakeStoreName -Path "$topLevelFolder/${client}_team_alert/" -AceType Group -id $accessGroupId -WarningAction ignore -Permissions $aclEntrybasePerm 
            Set-AzureRmDataLakeStoreItemAclEntry -Account $dataLakeStoreName -Path "$topLevelFolder/${client}_team_alert/" -AceType Group -id $accessGroupId -WarningAction ignore -Permissions $aclEntrybasePerm -Default
            write-host "$aclEntrybasePerm permission has been granted to $accessGroupName in ADLS $dataLakeStoreName$topLevelFolder/${client}_team_alert/"
        }
    }
}elseif ($topLevelFolder -eq "/datavault" -And $ListFile -like "*client_list.txt"){
    ForEach ($client in $clientList){
        $client = $client.replace("-","_")
        $client = $client.toLower()
        # Create new DATAVAULT schema directory and apply permission
        try {
            $clientExist = Get-AzureRmDataLakeStoreChildItem -AccountName $dataLakeStoreName -Path "$topLevelFolder/$client/" -ErrorAction Stop
        }
        catch {
            $newFolder = New-AzureRmDataLakeStoreItem -AccountName $dataLakeStoreName -Folder "$topLevelFolder/$client/"
            write-host "New DATAVAULT created at Storage Account $dataLakeStoreName path $newFolder"
        }
        Set-AzureRmDataLakeStoreItemOwner -Account $dataLakeStoreName -Path "$topLevelFolder/$client/" -Type Group -id $adminGroupId  -WarningAction Ignore 
        if ($accessGroupNameExist) {
            $accessGroupId = (Get-AzureRmADGroup -SearchString $accessGroupName|Select-Object -First 1).Id
            Set-AzureRmDataLakeStoreItemAclEntry -Account $dataLakeStoreName -Path "$topLevelFolder/$client/" -AceType Group -id $accessGroupId -WarningAction ignore -Permissions $aclEntrybasePerm 
            Set-AzureRmDataLakeStoreItemAclEntry -Account $dataLakeStoreName -Path "$topLevelFolder/$client/" -AceType Group -id $accessGroupId -WarningAction ignore -Permissions $aclEntrybasePerm -Default
            write-host "$aclEntrybasePerm permission has been granted to $accessGroupName in ADLS $dataLakeStoreName$topLevelFolder/$client/"
        }
    }
}elseif ($topLevelFolder -eq "/informationmart" -And $ListFile -like "*client_list.txt") {
    ForEach ($client in $clientList){
        $client = $client.replace("-","_")
        $client = $client.toLower()
        # Create new DATAVAULT schema directory for teams and apply permission
        try {
            $clientExist = Get-AzureRmDataLakeStoreChildItem -AccountName $dataLakeStoreName -Path "$topLevelFolder/${client}_retail_alert/" -ErrorAction Stop
        }
        catch {
            $newFolder = New-AzureRmDataLakeStoreItem -AccountName $dataLakeStoreName -Folder "$topLevelFolder/${client}_retail_alert/"
            write-host "New client INFORMATIONMART created at Storage Account $dataLakeStoreName path $newFolder"
        } 
        Set-AzureRmDataLakeStoreItemOwner -Account $dataLakeStoreName -Path "$topLevelFolder/${client}_retail_alert/" -Type Group -id $adminGroupId  -WarningAction Ignore 
        if ($accessGroupNameExist) {
            $accessGroupId = (Get-AzureRmADGroup -SearchString $accessGroupName|Select-Object -First 1).Id
            Set-AzureRmDataLakeStoreItemAclEntry -Account $dataLakeStoreName -Path "$topLevelFolder/${client}_retail_alert/" -AceType Group -id $accessGroupId -WarningAction ignore -Permissions $aclEntrybasePerm 
            Set-AzureRmDataLakeStoreItemAclEntry -Account $dataLakeStoreName -Path "$topLevelFolder/${client}_retail_alert/" -AceType Group -id $accessGroupId -WarningAction ignore -Permissions $aclEntrybasePerm -Default
            write-host "$aclEntrybasePerm permission has been granted to $accessGroupName in ADLS $dataLakeStoreName$topLevelFolder/${client}_retail_alert/"
        }
    }
}else {
    write-host "NOT FOUND TOP LEVEL FOLDER OR LIST FILE. ABORTED OPERATION"
    exit 8
}