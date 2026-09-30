$leader = "http://localhost:8080/raft/jobs"

$totalJobs = 10000
$success = 0
$failed = 0

$results = @()

Write-Host "========================================"
Write-Host "   RAFT 10,000 JOB CHAOS TEST"
Write-Host "========================================"
Write-Host ""

$startTime = Get-Date

for ($i = 1; $i -le $totalJobs; $i++) {

    $jobId = "chaos-{0:D5}" -f $i

    $body = @{
        jobId = $jobId
        type = "NORMAL"
        payload = "Chaos test job $i"
    } | ConvertTo-Json

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
        }

    }
    catch {
    $failed++
    Write-Host "FAILED $jobId : $($_.Exception.Message)"
}

    if ($i % 100 -eq 0) {

        Write-Host "Progress: $i / $totalJobs | Success: $success | Failed: $failed"

    }
}

$endTime = Get-Date
$duration = $endTime - $startTime

Write-Host ""
Write-Host "========================================"
Write-Host "             TEST COMPLETE"
Write-Host "========================================"
Write-Host "Submitted : $totalJobs"
Write-Host "Success   : $success"
Write-Host "Failed    : $failed"
Write-Host "Duration  : $duration"
Write-Host "========================================"