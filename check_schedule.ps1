$task = Get-ScheduledTask -TaskName 'CryptoLiveTrading'
Write-Host "Task: $($task.TaskName)"
Write-Host "State: $($task.State)"
Write-Host "Start: $($task.Triggers[0].StartBoundary)"
