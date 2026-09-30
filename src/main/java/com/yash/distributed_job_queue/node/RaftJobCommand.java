package com.yash.distributed_job_queue.node;

public class RaftJobCommand {

    private String jobId;
    private String type;
    private String payload;

    public RaftJobCommand() {
    }

    public RaftJobCommand(
            String jobId,
            String type,
            String payload) {

        this.jobId = jobId;
        this.type = type;
        this.payload = payload;
    }

    public String getJobId() {
        return jobId;
    }

    public String getType() {
        return type;
    }

    public String getPayload() {
        return payload;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }
}