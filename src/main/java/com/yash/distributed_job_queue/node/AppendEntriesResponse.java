package com.yash.distributed_job_queue.node;

public class AppendEntriesResponse {

    private int term;
    private boolean success;
    private int matchIndex;

    /*
     * Used for efficient Raft log backtracking.
     *
     * If the follower rejects AppendEntries because
     * the previous log entry does not match, it tells
     * the leader where the conflict begins.
     */
    private int conflictIndex;

    public AppendEntriesResponse() {
    }

    public AppendEntriesResponse(
            int term,
            boolean success,
            int matchIndex) {

        this.term = term;
        this.success = success;
        this.matchIndex = matchIndex;
        this.conflictIndex = 0;
    }

    public AppendEntriesResponse(
            int term,
            boolean success,
            int matchIndex,
            int conflictIndex) {

        this.term = term;
        this.success = success;
        this.matchIndex = matchIndex;
        this.conflictIndex = conflictIndex;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public int getMatchIndex() {
        return matchIndex;
    }

    public void setMatchIndex(int matchIndex) {
        this.matchIndex = matchIndex;
    }

    public int getConflictIndex() {
        return conflictIndex;
    }

    public void setConflictIndex(int conflictIndex) {
        this.conflictIndex = conflictIndex;
    }

    @Override
    public String toString() {
        return "AppendEntriesResponse{" +
                "term=" + term +
                ", success=" + success +
                ", matchIndex=" + matchIndex +
                ", conflictIndex=" + conflictIndex +
                '}';
    }
}