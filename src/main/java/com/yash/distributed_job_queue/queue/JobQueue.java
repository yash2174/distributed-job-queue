package com.yash.distributed_job_queue.queue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yash.distributed_job_queue.model.Job;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import com.yash.distributed_job_queue.wal.WriteAheadLog;
@Component
public class JobQueue {

    private static final String QUEUE_KEY = "job-queue";
    private static final String PROCESSING_QUEUE_KEY = "job-processing";
    private static final String DLQ_KEY = "dead-letter-queue";
    private final WriteAheadLog wal;
    
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    public JobQueue(
        StringRedisTemplate redisTemplate,
        ObjectMapper objectMapper,
        WriteAheadLog wal) {

    this.redisTemplate = redisTemplate;
    this.objectMapper = objectMapper;
    this.wal = wal;
    }

    public void add(Job job) {

        try {
            String jobJson = objectMapper.writeValueAsString(job);

            wal.write(jobJson);
            // Store job details
            redisTemplate.opsForValue()
                    .set("job:" + job.getId(), jobJson);

            // Add job to Redis queue
            redisTemplate.opsForList()
                    .rightPush(QUEUE_KEY, jobJson);

        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to add job", e);
        }
    }

    public Job take() throws InterruptedException {

    String jobJson = redisTemplate.opsForList()
            .leftPop(QUEUE_KEY);

    if (jobJson == null) {
        return null;
    }

    // Move job into processing queue
    redisTemplate.opsForList()
            .rightPush(PROCESSING_QUEUE_KEY, jobJson);

    try {
        return objectMapper.readValue(jobJson, Job.class);
    } catch (JsonProcessingException e) {
        throw new RuntimeException("Failed to read job", e);
    }
}

    public int size() {
        Long size = redisTemplate.opsForList()
                .size(QUEUE_KEY);

        return size != null ? size.intValue() : 0;
    }

    public Job getJob(String id) {

        try {
            String jobJson = redisTemplate.opsForValue()
                    .get("job:" + id);

            if (jobJson == null) {
                return null;
            }

            return objectMapper.readValue(jobJson, Job.class);

        } catch (Exception e) {
            throw new RuntimeException("Failed to get job", e);
        }
    }

    public void updateJob(Job job) {

        try {
            String jobJson = objectMapper.writeValueAsString(job);

            redisTemplate.opsForValue()
                    .set("job:" + job.getId(), jobJson);

        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to update job", e);
        }
    }
    public void removeFromProcessing(String jobId) {

    String jobJson = redisTemplate.opsForList()
            .leftPop(PROCESSING_QUEUE_KEY);

    if (jobJson == null) {
        return;
    }

    try {
        Job job = objectMapper.readValue(jobJson, Job.class);

        if (!job.getId().equals(jobId)) {
            // Put it back if it is not the requested job
            redisTemplate.opsForList()
                    .leftPush(PROCESSING_QUEUE_KEY, jobJson);
        }

    } catch (JsonProcessingException e) {
        throw new RuntimeException("Failed to remove job from processing queue", e);
    }
}
public void recoverProcessingJobs() {

    while (true) {

        String jobJson = redisTemplate.opsForList()
                .leftPop(PROCESSING_QUEUE_KEY);

        if (jobJson == null) {
            break;
        }

        redisTemplate.opsForList()
                .rightPush(QUEUE_KEY, jobJson);

        System.out.println("Recovered job from processing queue");
    }
}
public void moveToDeadLetterQueue(Job job) {
    try {
        String jobJson = objectMapper.writeValueAsString(job);

        redisTemplate.opsForList()
                .rightPush(DLQ_KEY, jobJson);

    } catch (JsonProcessingException e) {
        throw new RuntimeException("Failed to move job to DLQ", e);
    }
}
public void recoverFromWAL() {

    for (String jobJson : wal.readAll()) {

        try {
            Job job = objectMapper.readValue(jobJson, Job.class);

            // Restore job only if it is not already completed
            Job existingJob = getJob(job.getId());

if (existingJob == null && !"COMPLETED".equals(job.getStatus())) {
                redisTemplate.opsForValue()
                        .set("job:" + job.getId(), jobJson);

                redisTemplate.opsForList()
                        .rightPush(QUEUE_KEY, jobJson);

                System.out.println(
                        "Recovered job from WAL: " + job.getId()
                );
            }

        } catch (JsonProcessingException e) {
            System.out.println("Skipping invalid WAL entry");
        }
    }
}
public synchronized void addIfNotExists(Job job) {

    Job existing =
            getJob(job.getId());

    if (existing != null) {

        System.out.println(
                "⚠️ Job already exists: "
                        + job.getId()
        );

        return;
    }

    add(job);
}
}