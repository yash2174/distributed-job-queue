package com.yash.distributed_job_queue.node;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yash.distributed_job_queue.model.Job;
import com.yash.distributed_job_queue.queue.JobQueue;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class RaftStateMachine {

    private final RaftNode raftNode;
    private final JobQueue jobQueue;
    private final ObjectMapper objectMapper;

    public RaftStateMachine(
            RaftNode raftNode,
            JobQueue jobQueue,
            ObjectMapper objectMapper) {

        this.raftNode = raftNode;
        this.jobQueue = jobQueue;
        this.objectMapper =
                objectMapper;
    }

    @Scheduled(fixedRate = 100)
    public synchronized void applyCommittedEntries() {

        List<RaftLogEntry> entries =
                raftNode.getEntriesToApply();

        for (RaftLogEntry entry : entries) {

            try {

                RaftJobCommand command =
                        objectMapper.readValue(
                                entry.getCommand(),
                                RaftJobCommand.class);

                Job job = new Job();

                job.setId(
                        command.getJobId());

                job.setType(
                        command.getType());

                job.setPayload(
                        command.getPayload());

                job.setStatus(
                        "QUEUED");

                job.setRetryCount(0);

                jobQueue.addIfNotExists(job);

                raftNode.markApplied(
                        entry.getIndex());

                System.out.println(
                        "⚙️ Applied committed Raft entry "
                                + entry.getIndex()
                                + " → job "
                                + command.getJobId()
                );

            } catch (Exception e) {

                System.out.println(
                        "❌ Failed to apply Raft entry "
                                + entry.getIndex()
                                + " : "
                                + e.getMessage());

                /*
                 * Do NOT mark lastApplied here.
                 *
                 * The entry must remain available
                 * for another application attempt.
                 */
            }
        }
    }
}