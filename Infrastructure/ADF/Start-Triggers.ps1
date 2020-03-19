function Start-Triggers {
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
            $TriggerName = $_
            Write-Host "trying to Start " $TriggerName
            $Started=$false
            $cnt=0
            do {
                $cnt++
                try {        
                    $out=Start-AzureRmDataFactoryV2Trigger -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $TriggerName -Force -Verbose
                    if(!$? -or $out -eq $null){
                        Write-Host "Failed to Start trigger $TriggerName"
                        throw "Failed to Start trigger $TriggerName"
                    }
                    Write-Host "##[debug] Started the trigger $TriggerName"
                    $Started=$true
                }catch {
                    if($cnt -eq $Maximum){
                        Write-Host "##[debug] Tried 3 times to start the trigger, yet failed" 
                        $FailedTriggers += $TriggerName
                    }
                    Write-host "##[warning] tried to Start $TriggerName trigger $cnt timed, maximum tries are $Maximum"
                    Write-Host $_.Exception.InnerException.Message #-ErrorAction Continue
                    Start-Sleep -s $TimeOut
                }
            } while (!$Started -and $cnt -lt $Maximum )
        }

        if($FailedTriggers){
            $FailedTriggers =  $FailedTriggers | Select-Object -Unique
            $strFailedTriggers = $FailedTriggers -join ","
            Write-Error "##[error] Failed to Start Triggers $strFailedTriggers"
        }else{
            Write-Host "##[debug] Successfully Started all the Triggers" 
        }
    }
}
