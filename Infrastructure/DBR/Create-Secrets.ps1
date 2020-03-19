param
(
    [parameter(Mandatory = $true)] [String] $scopeName,
    [parameter(Mandatory = $true)] [String] $location,
    [parameter(Mandatory = $true)] [String] $databricksAccessToken,
    [parameter(Mandatory = $true)] [String] $resourceGroupName,
    [parameter(Mandatory = $true)] [String] $ApplicationStorageAccount
)
<#
    This code can only run against an active cluster. Cannot pre-install for all clusters.
    ./Create-Secrets.ps1 -scopeName "blob" -location "East US 2" -databricksAccessToken $databricksAccessToken -ApplicationStorageAccount azueus2devsaretfceng
#>
$location = $location.replace(" ",'').toLower()
# You can write your azure powershell scripts inline here. 
# You can also pass predefined and custom variables to this script using arguments


#Secret Scope
$uri ="https://$location.azuredatabricks.net/api/2.0/secrets/scopes/create"
$hdrs = @{}
$hdrs.Add("Authorization","Bearer $databricksAccessToken")
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$Body = @"
{
    "scope": "$scopeName",
    "initial_manage_principal": "users"
}
"@

Write-Output "Creating secret scope $scopeName"
try {
    $resp = Invoke-RestMethod -Uri $uri -Body $Body -Method 'POST' -Headers $hdrs -ErrorAction Continue
} catch {
   Write-Host $_ -fore green
}

#Secrets
$uri ="https://$location.azuredatabricks.net/api/2.0/secrets/put"

$key = "dbr-storage-extraconfigs-key"
$value ="fs.azure.account.key.$ApplicationStorageAccount.blob.core.windows.net"
$Body = @"
{
    "scope": "$scopeName",
    "key": "$key",
    "string_value": "$value"
}
"@
    Write-Output "Creating secret $key in scope $scopeName"
    Invoke-RestMethod -Uri $uri -Body $Body -Method 'POST' -Headers $hdrs

$key = "dbr-storage-extraconfigs-value"
$value = (Get-AzureRmStorageAccountKey -ResourceGroupName $resourceGroupName -Name $ApplicationStorageAccount)[0].Value
$Body = @"
{
    "scope": "$scopeName",
    "key": "$key",
    "string_value": "$value"
}
"@
    Write-Output "Creating secret $key in scope $scopeName"
    Invoke-RestMethod -Uri $uri -Body $Body -Method 'POST' -Headers $hdrs
    
    
$key = "dbr-storage-alertgeneration-source"
$value ="wasbs://dataplatform-retail-alertgeneration@$ApplicationStorageAccount.blob.core.windows.net"
$Body = @"
{
    "scope": "$scopeName",
    "key": "$key",
    "string_value": "$value"
}
"@
    Write-Output "Creating secret $key in scope $scopeName"
    Invoke-RestMethod -Uri $uri -Body $Body -Method 'POST' -Headers $hdrs


<#
    $hdrs = @{}
    $hdrs.Add("Authorization","Bearer $databricksAccessToken")
    $uri ="https://eastus2.azuredatabricks.net/api/2.0/secrets/list?scope=$scopeName"
    $resp =Invoke-RestMethod -Uri $uri -Method 'GET' -Headers $hdrs
    $resp |convertto-json
#>