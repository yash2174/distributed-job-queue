package com.yash.distributed_job_queue.worker;

import com.yash.distributed_job_queue.model.Job;
import com.yash.distributed_job_queue.queue.JobQueue;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class JobWorker implements CommandLineRunner {

    private static final int MAX_RETRIES = 3;

    private final JobQueue jobQueue;

    private final String workerId =
            java.util.UUID.randomUUID().toString();

    public JobWorker(JobQueue jobQueue) {
        this.jobQueue = jobQueue;
    }

    @Override
    public void run(String... args) throws Exception {

        jobQueue.recoverFromWAL();
        jobQueue.recoverProcessingJobs();

        while (true) {

            Job job = jobQueue.take();

            if (job == null) {
                Thread.sleep(1000);
                continue;
            }

            System.out.println(
                    "Worker " + workerId +
                    " processing job: " + job.getId()
            );

            job.setStatus("PROCESSING");
            jobQueue.updateJob(job);

            try {

                Thread.sleep(2000);

                if ("FAIL".equals(job.getType())) {
                    throw new RuntimeException("Test job failure");
                }

                job.setStatus("COMPLETED");

                jobQueue.updateJob(job);

                jobQueue.removeFromProcessing(job.getId());

                System.out.println(
                        "Worker " + workerId +
                        " completed job: " + job.getId()
                );

            } catch (Exception e) {

                job.setRetryCount(
                        job.getRetryCount() + 1
                );

                if (job.getRetryCount() <= MAX_RETRIES) {

                    job.setStatus("RETRYING");

                    jobQueue.updateJob(job);

                    jobQueue.removeFromProcessing(
                            job.getId()
                    );

                    jobQueue.add(job);

                    System.out.println(
                            "Job failed. Retrying: "
                            + job.getId()
                            + " Attempt: "
                            + job.getRetryCount()
                    );

                } else {

                    job.setStatus("FAILED");

                    jobQueue.updateJob(job);

                    jobQueue.removeFromProcessing(
                            job.getId()
                    );

                    jobQueue.moveToDeadLetterQueue(job);

                    System.out.println(
                            "Job permanently failed and moved to DLQ: "
                            + job.getId()
                    );
                }
            }
        }
    }
}