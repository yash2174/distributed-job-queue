package com.yash.distributed_job_queue.node;

public class RaftLogEntry {

    private int index;
    private int term;
    private String command;

    public RaftLogEntry() {
    }

    public RaftLogEntry(
            int index,
            int term,
            String command) {

        this.index = index;
        this.term = term;
        this.command = command;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    @Override
    public String toString() {

        return "RaftLogEntry{" +
                "index=" + index +
                ", term=" + term +
                ", command='" + command + '\'' +
                '}';
    }
}