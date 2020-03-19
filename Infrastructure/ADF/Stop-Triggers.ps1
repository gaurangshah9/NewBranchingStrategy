function Stop-Triggers {
    [CmdletBinding()]
    Param(
        [parameter(Mandatory = $true)] [String] $ResourceGroupName,
        [parameter(Mandatory = $true)] [String] $DataFactoryName,
        [parameter(Mandatory = $true)] [String[]] $Triggers,
        [Parameter(Position=1, Mandatory=$false)] [int] $Maximum = 3,
        [Parameter(Position=1, Mandatory=$false)] [int] $TimeOut = 30
    )

    Process {
        $FailedTriggers=@()
        $triggers | ForEach-Object { 
            $cnt = 0
            $TriggerName = $_
            Write-Host "trying to stop " $TriggerName
            $Stopped=$false
            do {
                $cnt++
                try {        
                    $out=Stop-AzureRmDataFactoryV2Trigger -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $TriggerName -Force -Verbose
                    if(!$? -or $out -eq $null){
                        Write-Host "Failed to Stop trigger $TriggerName"
                        throw "Failed to Stop trigger $TriggerName"
                    }
                    Write-Host "##[debug] Stopped the trigger $TriggerName"
                    $Stopped=$true
                }catch {
                    if($cnt -eq $Maximum){
                        $FailedTriggers += $TriggerName
                        Write-host "##[warning] tried to Stop $TriggerName trigger $cnt timed, maximum tries are $Maximum"
                    }
                    Write-Host $_.Exception.InnerException.Message #-ErrorAction Continue
                    Start-Sleep -s $TimeOut
                }
            } while ($cnt -lt $Maximum -and !$Stopped)
        }

        if($FailedTriggers){
            $FailedTriggers =  $FailedTriggers | Select-Object -Unique
            $strFailedTriggers = $FailedTriggers -join ","
            Write-Error "##[error] Failed to Stop Triggers $strFailedTriggers"
        }else{
           Write-Host "##[debug] Stopped All the Triggers Sucessfully"
        }
    }
}
