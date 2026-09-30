package com.yash.distributed_job_queue.node;

public class RaftReplicationState {

    private int nextIndex;
    private int matchIndex;

    public RaftReplicationState(int nextIndex) {
        this.nextIndex = nextIndex;
        this.matchIndex = 0;
    }

    public int getNextIndex() {
        return nextIndex;
    }

    public void setNextIndex(int nextIndex) {
        this.nextIndex = nextIndex;
    }

    public int getMatchIndex() {
        return matchIndex;
    }

    public void setMatchIndex(int matchIndex) {
        this.matchIndex = matchIndex;
    }
}