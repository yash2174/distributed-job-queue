package com.yash.distributed_job_queue.node;

public class RaftState {

    public enum Role {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }

    private int currentTerm = 0;
    private String votedFor = null;
    private Role role = Role.FOLLOWER;

    private int commitIndex = 0;
    private int lastApplied = 0;

    public int getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(int currentTerm) {
        this.currentTerm = currentTerm;
    }

    public String getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(String votedFor) {
        this.votedFor = votedFor;
    }

    public Role getRole() {
        return role;
    }

    public void setRole(Role role) {
        this.role = role;
    }

    public int getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(int commitIndex) {
        this.commitIndex = commitIndex;
    }

    public int getLastApplied() {
        return lastApplied;
    }

    public void setLastApplied(int lastApplied) {
        this.lastApplied = lastApplied;
    }
}
