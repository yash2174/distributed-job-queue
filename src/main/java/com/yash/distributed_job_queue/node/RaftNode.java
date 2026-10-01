package com.yash.distributed_job_queue.node;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class RaftNode {

    private final RaftState state = new RaftState();
    private final RaftPersistentLog persistentLog;
    private final RaftMetadataStore metadataStore;

    private final List<RaftLogEntry> log =
            new ArrayList<>();

    @Value("${raft.node-id}")
    private String nodeId;

    public RaftNode(
            RaftPersistentLog persistentLog,
            RaftMetadataStore metadataStore) {

        this.persistentLog = persistentLog;
        this.metadataStore = metadataStore;

        // Restore persistent Raft log
        this.log.addAll(
                persistentLog.getAll());

        // Restore persistent Raft metadata
        RaftMetadata metadata =
                metadataStore.load();

        state.setCurrentTerm(
                metadata.getCurrentTerm());

        state.setVotedFor(
                metadata.getVotedFor());

        state.setCommitIndex(
                metadata.getCommitIndex());

        state.setLastApplied(
                metadata.getLastApplied());

        System.out.println(
                "📖 Restored Raft state"
                        + " | term="
                        + state.getCurrentTerm()
                        + " | commit="
                        + state.getCommitIndex()
                        + " | applied="
                        + state.getLastApplied());
    }

    private void persistState() {

        RaftMetadata metadata =
                new RaftMetadata();

        metadata.setCurrentTerm(
                state.getCurrentTerm());

        metadata.setVotedFor(
                state.getVotedFor());

        metadata.setCommitIndex(
                state.getCommitIndex());

        metadata.setLastApplied(
                state.getLastApplied());

        metadataStore.save(metadata);
    }


    public synchronized String getNodeId() {
        return nodeId;
    }

    public synchronized RaftState.Role getRole() {
        return state.getRole();
    }

    public synchronized int getCurrentTerm() {
        return state.getCurrentTerm();
    }

    public synchronized String getVotedFor() {
        return state.getVotedFor();
    }

    public synchronized int getCommitIndex() {
        return state.getCommitIndex();
    }

    public synchronized int getLastApplied() {
        return state.getLastApplied();
    }

    public synchronized int getLastLogIndex() {

        if (log.isEmpty()) {
            return 0;
        }

        return log.get(log.size() - 1).getIndex();
    }

    public synchronized int getLastLogTerm() {

        if (log.isEmpty()) {
            return 0;
        }

        return log.get(log.size() - 1).getTerm();
    }

  
    public synchronized void startElection() {

        state.setRole(
                RaftState.Role.CANDIDATE);

        state.setCurrentTerm(
                state.getCurrentTerm() + 1);

        state.setVotedFor(nodeId);

        // Persist new term and vote
        persistState();

        System.out.println(
                "Election started by "
                        + nodeId
                        + " | term="
                        + state.getCurrentTerm());
    }

    public synchronized RequestVoteResponse
    handleRequestVote(
            RequestVoteRequest request) {

        // Reject old term
        if (request.getTerm()
                < state.getCurrentTerm()) {

            return new RequestVoteResponse(
                    state.getCurrentTerm(),
                    false);
        }

        // Newer term discovered
        if (request.getTerm()
                > state.getCurrentTerm()) {

            state.setCurrentTerm(
                    request.getTerm());

            state.setRole(
                    RaftState.Role.FOLLOWER);

            state.setVotedFor(null);

            // Persist newer term
            persistState();
        }

        boolean logIsUpToDate =
                isCandidateLogUpToDate(
                        request.getLastLogIndex(),
                        request.getLastLogTerm());

        boolean canVote =
                state.getVotedFor() == null
                        || state.getVotedFor()
                        .equals(request.getCandidateId());

        if (canVote && logIsUpToDate) {

            state.setVotedFor(
                    request.getCandidateId());

            // Persist vote
            persistState();

            System.out.println(
                    nodeId
                            + " granted vote to "
                            + request.getCandidateId()
                            + " | term="
                            + state.getCurrentTerm());

            return new RequestVoteResponse(
                    state.getCurrentTerm(),
                    true);
        }

        return new RequestVoteResponse(
                state.getCurrentTerm(),
                false);
    }

    private boolean isCandidateLogUpToDate(
            int candidateLastLogIndex,
            int candidateLastLogTerm) {

        int myLastLogTerm =
                getLastLogTerm();

        int myLastLogIndex =
                getLastLogIndex();

        if (candidateLastLogTerm
                != myLastLogTerm) {

            return candidateLastLogTerm
                    > myLastLogTerm;
        }

        return candidateLastLogIndex
                >= myLastLogIndex;
    }

   
    public synchronized AppendEntriesResponse handleAppendEntries(
        AppendEntriesRequest request) {

   
    if (request.getTerm() < state.getCurrentTerm()) {

        return new AppendEntriesResponse(
                state.getCurrentTerm(),
                false,
                getLastLogIndex(),
                getLastLogIndex() + 1
        );
    }

    
    if (request.getTerm() > state.getCurrentTerm()) {

        state.setCurrentTerm(request.getTerm());
        state.setVotedFor(null);

        persistState();
    }

   
    state.setRole(RaftState.Role.FOLLOWER);

    
    if (request.getPrevLogIndex() > getLastLogIndex()) {

        int conflictIndex = getLastLogIndex() + 1;

        return new AppendEntriesResponse(
                state.getCurrentTerm(),
                false,
                getLastLogIndex(),
                conflictIndex
        );
    }

    
    if (request.getPrevLogIndex() > 0) {

        RaftLogEntry previousEntry =
                getLogEntryPublic(request.getPrevLogIndex());

        if (previousEntry == null) {

            int conflictIndex =
                    Math.min(
                            request.getPrevLogIndex(),
                            getLastLogIndex() + 1
                    );

            return new AppendEntriesResponse(
                    state.getCurrentTerm(),
                    false,
                    getLastLogIndex(),
                    conflictIndex
            );
        }

        if (previousEntry.getTerm()
                != request.getPrevLogTerm()) {

           
            int conflictTerm = previousEntry.getTerm();

            int conflictIndex = request.getPrevLogIndex();

            while (conflictIndex > 1) {

                RaftLogEntry previous =
                        getLogEntryPublic(conflictIndex - 1);

                if (previous == null
                        || previous.getTerm() != conflictTerm) {
                    break;
                }

                conflictIndex--;
            }

            return new AppendEntriesResponse(
                    state.getCurrentTerm(),
                    false,
                    getLastLogIndex(),
                    conflictIndex
            );
        }
    }

 
    List<RaftLogEntry> incoming =
            request.getEntries();

    if (incoming != null && !incoming.isEmpty()) {

        for (RaftLogEntry incomingEntry : incoming) {

            RaftLogEntry existing =
                    getLogEntryPublic(
                            incomingEntry.getIndex());

            if (existing != null
                    && existing.getTerm()
                    != incomingEntry.getTerm()) {

                removeEntriesFrom(
                        incomingEntry.getIndex());

                break;
            }
        }

        for (RaftLogEntry incomingEntry : incoming) {

            RaftLogEntry existing =
                    getLogEntryPublic(
                            incomingEntry.getIndex());

            if (existing == null) {
                appendLocalEntry(incomingEntry);
            }
        }
    }

    /*
     * Update commit index from leader.
     */
    if (request.getLeaderCommit()
            > state.getCommitIndex()) {

        int newCommitIndex =
                Math.min(
                        request.getLeaderCommit(),
                        getLastLogIndex()
                );

        state.setCommitIndex(newCommitIndex);

        persistState();
    }

    int matchIndex = getLastLogIndex();

    return new AppendEntriesResponse(
            state.getCurrentTerm(),
            true,
            matchIndex,
            0
    );
}

    
    private RaftLogEntry getLogEntry(
            int index) {

        for (RaftLogEntry entry : log) {

            if (entry.getIndex() == index) {
                return entry;
            }
        }

        return null;
    }

    private void removeEntriesFrom(
            int index) {

        log.removeIf(
                entry ->
                        entry.getIndex()
                                >= index);

        persistentLog.replaceFrom(
                index,
                new ArrayList<>());
    }

    public synchronized void appendLocalEntry(
            RaftLogEntry entry) {

        log.add(entry);

        persistentLog.append(entry);

        System.out.println(
                nodeId
                        + " persisted log entry "
                        + entry.getIndex()
                        + " | term="
                        + entry.getTerm());
    }

    public synchronized RaftLogEntry
    getLogEntryPublic(int index) {

        return getLogEntry(index);
    }

    
    public synchronized void advanceCommitIndex(
            int index) {

        if (index > state.getCommitIndex()
                && index <= getLastLogIndex()) {

            state.setCommitIndex(index);

            // Persist commit index
            persistState();

            System.out.println(
                    "✅ "
                            + nodeId
                            + " commitIndex advanced to "
                            + index);
        }
    }

    

    public synchronized List<RaftLogEntry>
    getEntriesToApply() {

        List<RaftLogEntry> result =
                new ArrayList<>();

        int nextIndex =
                state.getLastApplied() + 1;

        while (nextIndex
                <= state.getCommitIndex()) {

            RaftLogEntry entry =
                    getLogEntry(nextIndex);

            if (entry != null) {
                result.add(entry);
            }

            nextIndex++;
        }

        return result;
    }

    public synchronized void markApplied(
            int index) {

        if (index > state.getLastApplied()) {

            state.setLastApplied(index);

            // Persist lastApplied
            persistState();
        }
    }


    public synchronized void becomeLeader() {

        state.setRole(
                RaftState.Role.LEADER);

        System.out.println(
                "🔥 "
                        + nodeId
                        + " became LEADER"
                        + " | term="
                        + state.getCurrentTerm());
    }

    public synchronized void becomeFollower(
            int term) {

        if (term > state.getCurrentTerm()) {

            state.setCurrentTerm(term);
            state.setVotedFor(null);

            // Persist newer term
            persistState();
        }

        state.setRole(
                RaftState.Role.FOLLOWER);

        System.out.println(
                "⬇️ "
                        + nodeId
                        + " became FOLLOWER"
                        + " | term="
                        + state.getCurrentTerm());
    }
}