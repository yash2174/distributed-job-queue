$leader = "http://localhost:8080/raft/jobs"

$totalJobs = 5000
$success = 0
$failed = 0

$startTime = Get-Date

Write-Host "========================================"
Write-Host "       RAFT 5,000 JOB TEST"
Write-Host "========================================"

for ($i = 1; $i -le $totalJobs; $i++) {

    $jobId = "chaos-{0:D5}" -f $i

    $body = @{
        jobId = $jobId
        type = "NORMAL"
        payload = "5000 job test $i"
    } | ConvertTo-Json -Compress

    try {

        $response = Invoke-RestMethod `
            -Uri $leader `
            -Method Post `
            -ContentType "application/json" `
            -Body $body

        if ($response -like "SUCCESS*") {
            $success++
        }
        else {
            $failed++
            Write-Host "FAILED: $jobId -> $response"
        }

    }
    catch {

        $failed++

        Write-Host "ERROR: $jobId -> $($_.Exception.Message)"
    }

    if ($i % 100 -eq 0) {

        Write-Host `
            "Progress: $i / $totalJobs | Success: $success | Failed: $failed"
    }
}

$duration = (Get-Date) - $startTime

Write-Host ""
Write-Host "========================================"
Write-Host "           TEST COMPLETE"
Write-Host "========================================"
Write-Host "Submitted : $totalJobs"
Write-Host "Success   : $success"
Write-Host "Failed    : $failed"
Write-Host "Duration  : $duration"
Write-Host "========================================"