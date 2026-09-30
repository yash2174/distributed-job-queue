package com.yash.distributed_job_queue.model;


public class Job {
    

    private String id;
    private String type;
    private String payload;
    private String status;

    private int retryCount;

    public Job() {
    }

    public Job(String id, String type, String payload, String status) {
        this.id = id;
        this.type = type;
        this.payload = payload;
        this.status = status;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
    public int getRetryCount() {
    return retryCount;
}

public void setRetryCount(int retryCount) {
    this.retryCount = retryCount;
}
}