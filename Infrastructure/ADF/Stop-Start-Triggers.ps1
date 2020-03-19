param
(
    [parameter(Mandatory = $true)] [String] $armTemplate,
    [parameter(Mandatory = $true)] [String] $ResourceGroupName,
    [parameter(Mandatory = $true)] [String] $DataFactoryName,
    [parameter(Mandatory = $false)] [Bool] $predeployment=$false

)

. "$PSScriptRoot/Stop-Triggers.ps1"
. "$PSScriptRoot/Start-Triggers.ps1"

$templateJson = Get-Content $armTemplate | ConvertFrom-Json
$resources = $templateJson.resources

Function Delete-ChildPipeLines{
    param(
        [parameter(Mandatory = $true)] [String []] $PiplinesToDelete
    )

    $failedToDelete = @()
    $PiplinesToDelete | ForEach-Object { 
        $pipelineName = $_
        Write-Host "##[Debug] Deleting PipeLine: $pipelineName"
        try{
            Remove-AzureRmDataFactoryV2Pipeline -Name $pipelineName -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Force -Verbose | Out-Host
            Write-Host "##[Debug] Sucessfully deleted pipline: $pipelineName"
        }catch{
            $errorMessage = $_.Exception.Message | out-string
            Write-Host "##[Warning] Failed to delete pipeline: $pipelineName"
            Write-Host $errorMessage
            $failedToDelete += $pipelineName
        }
    }

    return $failedToDelete

}

function Delete-Triggers {
    $triggersADF = Get-AzureRmDataFactoryV2Trigger -DataFactoryName $DataFactoryName -ResourceGroupName $ResourceGroupName
    $triggersTemplate = $resources | Where-Object { $_.type -eq "Microsoft.DataFactory/factories/triggers" }
    $deletedTriggers = $triggersADF | Where-Object { $triggerNames -notcontains $_.Name }

    #delte resources
    Write-Host "Deleting triggers"
    $deletedTriggers | ForEach-Object { 
        $TriggerName = $_.Name
        Write-Host "##[Debug] Deleting Trigger: $TriggerName"
        try{
            Remove-AzureRmDataFactoryV2Trigger -Name $TriggerName -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Force -Verbose  | Out-Host
            Write-Host "##[Debug] Sucessfully deleted Trigger: $TriggerName"
        }catch{
            Write-Host "##[warning] Failed to delete Trigger: $TriggerName"
            $ErrorMessage = $_.Exception.Message
            Write-Host $_.Exception.Message
        }
     }    
}


function Delete-Pipelines {
    
    #pipelines
    Write-Host "Getting pipelines"
    $pipelinesADF = Get-AzureRmDataFactoryV2Pipeline -DataFactoryName $DataFactoryName -ResourceGroupName $ResourceGroupName
    $pipelinesTemplate = $resources | Where-Object { $_.type -eq "Microsoft.DataFactory/factories/pipelines" }
    $pipelinesNames = $pipelinesTemplate | ForEach-Object {$_.name.Substring(37, $_.name.Length-40)}
    $deletedPipelines = $pipelinesADF | Where-Object { $pipelinesNames -notcontains $_.Name }
    $pipelineNames = @() 
    $deletedPipelines | ForEach-Object {
        $pipelineNames += $_.Name 
    }
    
    if ($pipelineNames){
        Write-host "pipelineNames = $pipelineNames"
        Write-Host "Deleting pipelines"
        $counter = 0 
        $failedToDelete = @()

        do{
            $failedToDelete = Delete-ChildPipeLines -PiplinesToDelete $pipelineNames
            $pipelineNames = $failedToDelete
            $counter ++
        }while ($counter -lt 3 -and $failedToDelete)   

        if($failedToDelete){
            $failedToDelete =  $failedToDelete | Select-Object -Unique
            $strFailedToDelete = $failedToDelete -join ","
            Write-Host "##[error] Failed to Delete Pipelines: $strFailedToDelete"
        }else{
           Write-Host "##[debug] Deleted All the Pipeline Sucessfully"
        }
    }
 
}

function Delete-DataSets {
    #datasets
    Write-Host "Getting datasets"
    $datasetsADF = Get-AzureRmDataFactoryV2Dataset -DataFactoryName $DataFactoryName -ResourceGroupName $ResourceGroupName
    $datasetsTemplate = $resources | Where-Object { $_.type -eq "Microsoft.DataFactory/factories/datasets" }
    $datasetsNames = $datasetsTemplate | ForEach-Object {$_.name.Substring(37, $_.name.Length-40) }
    $deletedDataset = $datasetsADF | Where-Object { $datasetsNames -notcontains $_.Name }

    Write-Host "Deleting datasets"
    $deletedDataset | ForEach-Object { 
        $DatasetName = $_.Name
        Write-Host "##[debug] Deleting DataSet: $DatasetName" 

        try{
            Remove-AzureRmDataFactoryV2Dataset -Name $DatasetName -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Force -Verbose | Out-Host
            Write-Host "##[debug] Sucessfully Deleted DataSet: $DatasetName" 
        }catch{
            Write-Host $_.Exception.InnerException.Message
        }
    }
}

function Delete-LinkedServices {
    #linkedservices
    Write-Host "Getting linked services"
    $linkedservicesADF = Get-AzureRmDataFactoryV2LinkedService -DataFactoryName $DataFactoryName -ResourceGroupName $ResourceGroupName
    $linkedservicesTemplate = $resources | Where-Object { $_.type -eq "Microsoft.DataFactory/factories/linkedservices" }
    $linkedservicesNames = $linkedservicesTemplate | ForEach-Object {$_.name.Substring(37, $_.name.Length-40)}
    $deletedLinkedServices = $linkedservicesADF | Where-Object { $linkedservicesNames -notcontains $_.Name }

    Write-Host "Deleting linked services"
    $deletedLinkedServices | ForEach-Object { 
        $ServiceName = $_.Name
        Write-Host "##[debug] Deleting LinkedServices: $ServiceName"  
        try{
            Remove-AzureRmDataFactoryV2LinkedService -Name $ServiceName -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Force -Verbose | Out-Host
            Write-Host "##[debug] Sucessfully Deleted LinkedServices: $ServiceName" 
        }catch{
            Write-Host "##[debug] Failed to Delete LinkedServices: $ServiceName" 
            Write-Host $_.Exception.Message
        }
    }
}

function Delete-IntegrationRuntimes {
    #Integrationruntimes
    Write-Host "Getting integration runtimes"
    $integrationRuntimesADF = Get-AzureRmDataFactoryV2IntegrationRuntime -DataFactoryName $DataFactoryName -ResourceGroupName $ResourceGroupName
    $integrationruntimesTemplate = $resources | Where-Object { $_.type -eq "Microsoft.DataFactory/factories/integrationruntimes" }
    $integrationruntimesNames = $integrationruntimesTemplate | ForEach-Object {$_.name.Substring(37, $_.name.Length-40)}
    $deletedIntegrationRuntimes = $integrationRuntimesADF | Where-Object { $integrationruntimesNames -notcontains $_.Name }


    Write-Host "Deleting integration runtimes"
    $deletedIntegrationRuntimes | ForEach-Object { 
        $Runtime = $_.Name
        Write-Host "##[debug] Deleting IntegrationRuntimes:  $Runtime"
        try{
            Remove-AzureRmDataFactoryV2IntegrationRuntime -Name $Runtime -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Force -Verbose   |Out-Host
            Write-Host "##[debug] Sucessfully deleted IntegrationRuntimes: $Runtime"
        }catch{
            Write-Host "##[warning] Failed to delete IntegrationRuntimes: $Runtime"
            Write-Host $_.Exception.InnerException.Message
        }
    } 
}

#Triggers 
Write-Host "Getting triggers"
$triggersADF = Get-AzureRmDataFactoryV2Trigger -DataFactoryName $DataFactoryName -ResourceGroupName $ResourceGroupName
$triggersTemplate = $resources | Where-Object { $_.type -eq "Microsoft.DataFactory/factories/triggers" }
$triggerNames = $triggersTemplate | ForEach-Object {$_.name.Substring(37, $_.name.Length-40)}
$activeTriggerNames = $triggersTemplate | Where-Object { $_.properties.runtimeState -eq "Started" -and $_.properties.pipelines.Count -gt 0} | ForEach-Object {$_.name.Substring(37, $_.name.Length-40)}

# $triggersToStop = $triggerNames | where { ($triggersADF | Select-Object name).name -contains $_ }
$triggersToStop = ($triggersADF | foreach-object {$_.Name})
if ($predeployment -eq $true) {

    if($triggersToStop){
        Write-Host "Stopping deployed triggers"
        Stop-Triggers  -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Triggers $triggersToStop
    } 
}
else {

    Delete-Triggers
    Delete-Pipelines
    Delete-DataSets 
    Delete-LinkedServices
    Delete-IntegrationRuntimes 
    Write-Host "##[debug] triggersTemplate $triggersTemplate"
    Write-Host "##[debug] active triggers: $activeTriggerNames"
    if($activeTriggerNames){
        Write-Host "Starting active triggers"
        Start-Triggers  -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Triggers $activeTriggerNames
    }
    
}
