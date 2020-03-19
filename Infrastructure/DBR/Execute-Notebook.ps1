param
(
    [parameter(Mandatory = $true)] [String] $location,
    [parameter(Mandatory = $true)] [String] $databricksAccessToken,
    [parameter(Mandatory = $true)] [String] $notebookPath = "/dataplatform-retail-alertgeneration/Utilities/MountDrives"
)
<#
    This code can only run against an active cluster. Cannot pre-install for all clusters.
    ./Execute-Notebook.ps1 -location "East US 2" -databricksAccessToken $databricksAccessToken -notebookPath "/dataplatform-retail-forecastengine/MountDrives"
#>
$location = $location.replace(" ",'').toLower()
# You can write your azure powershell scripts inline here. 
# You can also pass predefined and custom variables to this script using arguments

#Create the Job
$uri ="https://$location.azuredatabricks.net/api/2.0/jobs/runs/submit"
$hdrs = @{}
$hdrs.Add("Authorization","Bearer $databricksAccessToken")
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$Body = @"
{
    "name": "Mount Drives",
    "new_cluster": {
      "spark_version": "4.3.x-scala2.11",
      "node_type_id": "Standard_D3_v2",
      "num_workers": 1
    },
    "libraries": [
    ],
    "timeout_seconds": 3600,
    "max_retries": 1,
    "notebook_task": {
      "notebook_path": "/dataplatform-retail-alertgeneration/Utilities/MountDrives"
    }
  }
"@

Write-Output "Running Mount Drives notebook"
$resp = Invoke-RestMethod -Uri $uri -Body $Body -Method 'POST' -Headers $hdrs
$jobId = $resp.run_id
Write-output "Kicked off job $jobId"
$uri ="https://$location.azuredatabricks.net/api/2.0/jobs/runs/get?run_id=$jobId"
$resp = Invoke-RestMethod -Uri $uri -Method 'GET' -Headers $hdrs
$resp | convertto-Json


<#
$hdrs = @{}
$hdrs.Add("Authorization","Bearer $databricksAccessToken")
$uri ="https://eastus2.azuredatabricks.net/api/2.0/jobs/runs/get?run_id=$jobId"
$resp = Invoke-RestMethod -Uri $uri -Method 'GET' -Headers $hdrs
$resp | convertto-Json
#>