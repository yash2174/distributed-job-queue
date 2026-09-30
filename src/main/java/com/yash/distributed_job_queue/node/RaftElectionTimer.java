package com.yash.distributed_job_queue.node;

import org.springframework.stereotype.Component;

import java.util.Random;

@Component
public class RaftElectionTimer {

    private final Random random = new Random();

    private volatile long electionDeadline;

    public RaftElectionTimer() {
        reset();
    }

    public synchronized void reset() {

        int timeout =
                500 + random.nextInt(501);

        electionDeadline =
                System.currentTimeMillis()
                        + timeout;
    }

    public boolean expired() {

        return System.currentTimeMillis()
                >= electionDeadline;
    }
}