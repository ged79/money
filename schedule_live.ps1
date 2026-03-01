$action = New-ScheduledTaskAction -Execute 'cmd.exe' -Argument '/c C:\Users\lungg\.openclaw\workspace\money\start_live.bat' -WorkingDirectory 'C:\Users\lungg\.openclaw\workspace\money'
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(90)
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName 'CryptoLiveTrading' -Action $action -Trigger $trigger -Settings $settings -Description 'SOL L4 Grid Live Trading Auto-Start' -Force
Write-Host "Scheduled! Start time: $((Get-Date).AddMinutes(90).ToString('HH:mm:ss'))"
