param (
    [parameter(Mandatory=$true)] [string] $ResourceGroupName ="azu-eus2-dev-rg-DataPlatformCoordinator",
    [parameter(Mandatory=$true)] [string] $CosmosDbAccountName = "azu-eus2-dev-cdb-dataplatcoord",
    [parameter(Mandatory=$true)] [string] $cosmosDBName = "coordination",
    [parameter(Mandatory=$true)] [string] $cosmosDBCollectionId = "linked_services",
    [parameter(Mandatory=$true)] [string] $documentsPath = "C:\Users\Paulo.Matioli\Documents\temp"
)

if (-Not (Get-Module -ListAvailable -Name CosmosDB)) {
    Install-Module -Name CosmosDB -Force -Scope CurrentUser -allowclobber 
}
#Remove-Module -name AzureRM -Force
#Uninstall-Module -name AzureRM.Profile -Force -Scope CurrentUser
Import-Module Az.Accounts
Enable-AzureRmAlias -Scope CurrentUser
Install-Module -Name CosmosDB -Force -Scope CurrentUser -allowclobber 
Import-Module -Name CosmosDB -Force
Get-InstalledModule -name CosmosDB

# Get a reference to the Cosmos Account
Write-host "Getting Context"
$cosmosDbContext = New-CosmosDbContext -Account $CosmosDbAccountName -Database $cosmosDBName -ResourceGroup $ResourceGroupName -MasterKeyType 'PrimaryMasterKey'
# Get list of Json documents
$documentsList = Get-ChildItem -Path $documentsPath

# Create each document into the Cosmos context
foreach ($file in $documentsList){
    $document = [IO.File]::ReadAllText("$documentsPath\$file")
    $documentId = $file.Name.Replace(".json","")
    $query = "SELECT * FROM customers c WHERE (c.id = '$documentId')"
    $documentExist = Get-CosmosDbDocument -context $cosmosDbContext -CollectionId $cosmosDBCollectionId -Query $query -ErrorVariable documentNotFound 
    if ($documentNotFound) {
        New-CosmosDbDocument -Context $cosmosDbContext -CollectionId $cosmosDBCollectionId -DocumentBody $document
        write-host "COMPLETED - Document $documentId was successfully created"
    }elseif($documentExist){
        Remove-CosmosDbDocument -Context $cosmosDbContext -CollectionId $cosmosDBCollectionId -Id $documentExist.Id
        New-CosmosDbDocument -Context $cosmosDbContext -CollectionId $cosmosDBCollectionId -DocumentBody $document
        write-host "Document $documentExist.id has been refreshed with ID $documentId"
    }else {
        New-CosmosDbDocument -Context $cosmosDbContext -CollectionId $cosmosDBCollectionId -DocumentBody $document
        write-host "Created newm document $documentId"
    }
    
}
