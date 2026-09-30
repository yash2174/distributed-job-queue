package com.yash.distributed_job_queue.controller;

import com.yash.distributed_job_queue.model.Job;
import com.yash.distributed_job_queue.queue.JobQueue;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/jobs")
public class JobController {

    private final JobQueue jobQueue;

    public JobController(JobQueue jobQueue) {
        this.jobQueue = jobQueue;
    }

    @PostMapping
    public Job createJob(@RequestBody Job job) {
        jobQueue.add(job);
        return job;
    }

    @GetMapping("/size")
    public int getQueueSize() {
        return jobQueue.size();
    }

    @GetMapping("/{id}")
    public Job getJob(@PathVariable String id) {
    return jobQueue.getJob(id);
}
}