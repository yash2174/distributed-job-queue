package com.yash.distributed_job_queue.node;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class RaftReplicationService {

    private final RaftNode raftNode;

    private final RestTemplate restTemplate =
            new RestTemplate();

    private final ObjectMapper objectMapper;

    /*
     * Per-follower replication state.
     *
     * nextIndex:
     * First log entry that should be sent to follower.
     *
     * matchIndex:
     * Highest log entry known to be replicated.
     */
    private final Map<String, RaftReplicationState>
            replicationState = new HashMap<>();

    @Value("${raft.peers}")
    private String peers;

    public RaftReplicationService(
            RaftNode raftNode,
            ObjectMapper objectMapper) {

        this.raftNode = raftNode;
        this.objectMapper = objectMapper;
    }

    /*
     * Initialize replication state for all followers.
     */
    private void initializeFollowerState() {

        int nextIndex =
                raftNode.getLastLogIndex() + 1;

        for (String peer : getPeers()) {

            replicationState.computeIfAbsent(
                    peer,
                    p -> new RaftReplicationState(nextIndex)
            );
        }
    }

    /*
     * =========================================================
     * CLIENT JOB REPLICATION
     * =========================================================
     *
     * Add a new command to the leader's log and replicate it.
     *
     * We only need a majority to commit.
     *
     * A slow follower does NOT block the commit once
     * a majority has acknowledged the entry.
     */
    public synchronized boolean replicate(
            String command) {

        if (raftNode.getRole()
                != RaftState.Role.LEADER) {

            return false;
        }

        initializeFollowerState();

        int newIndex =
                raftNode.getLastLogIndex() + 1;

        int currentTerm =
                raftNode.getCurrentTerm();

        RaftLogEntry entry =
                new RaftLogEntry(
                        newIndex,
                        currentTerm,
                        command
                );

        /*
         * First persist locally.
         */
        raftNode.appendLocalEntry(entry);

        int acknowledgements = 1;

        int clusterSize =
                getPeers().size() + 1;

        int majority =
                (clusterSize / 2) + 1;

        System.out.println(
                "📨 Replicating log entry "
                        + newIndex
                        + " | term="
                        + currentTerm
        );

        /*
         * Try followers until majority is reached.
         *
         * This is the important improvement:
         * a lagging follower no longer prevents a
         * majority commit.
         */
        for (String peer : getPeers()) {

            if (replicateToFollower(peer)) {

                acknowledgements++;

                System.out.println(
                        "✅ Replication ACK from "
                                + peer
                                + " | index="
                                + newIndex
                );

                if (acknowledgements >= majority) {
                    break;
                }
            }
        }

        System.out.println(
                "Replication ACKs: "
                        + acknowledgements
                        + "/"
                        + clusterSize
                        + " | majority="
                        + majority
        );

        /*
         * Majority achieved.
         */
        if (acknowledgements >= majority) {

            /*
             * Only current-term entries should be directly
             * committed by this leader.
             */
            if (entry.getTerm()
                    == raftNode.getCurrentTerm()) {

                raftNode.advanceCommitIndex(
                        newIndex
                );

                System.out.println(
                        "🟢 Log entry "
                                + newIndex
                                + " COMMITTED"
                );

                return true;
            }
        }

        System.out.println(
                "🔴 Log entry "
                        + newIndex
                        + " NOT COMMITTED"
        );

        return false;
    }

    /*
     * =========================================================
     * REPLICATE ONE FOLLOWER
     * =========================================================
     *
     * Performs ONE AppendEntries RPC.
     *
     * No recursion.
     *
     * If follower is behind, conflictIndex lets the leader
     * jump directly toward the correct position.
     */
    public synchronized boolean replicateToFollower(
            String peer) {

        if (raftNode.getRole()
                != RaftState.Role.LEADER) {

            return false;
        }

        RaftReplicationState state =
                replicationState.get(peer);

        if (state == null) {

            state =
                    new RaftReplicationState(
                            raftNode.getLastLogIndex() + 1
                    );

            replicationState.put(
                    peer,
                    state
            );
        }

        int nextIndex =
                state.getNextIndex();

        int lastIndex =
                raftNode.getLastLogIndex();

        /*
         * Keep nextIndex within valid range.
         */
        if (nextIndex < 1) {
            nextIndex = 1;
            state.setNextIndex(1);
        }

        if (nextIndex > lastIndex + 1) {

            nextIndex = lastIndex + 1;

            state.setNextIndex(
                    nextIndex
            );
        }

        int prevLogIndex =
                nextIndex - 1;

        int prevLogTerm = 0;

        if (prevLogIndex > 0) {

            RaftLogEntry previousEntry =
                    raftNode.getLogEntryPublic(
                            prevLogIndex
                    );

            if (previousEntry == null) {

                state.setNextIndex(
                        Math.max(1, nextIndex - 1)
                );

                return false;
            }

            prevLogTerm =
                    previousEntry.getTerm();
        }

        /*
         * Build a batch of entries.
         *
         * Instead of sending one entry at a time,
         * send everything currently needed.
         */
        List<RaftLogEntry> entries =
                new ArrayList<>();

        for (int index = nextIndex;
             index <= lastIndex;
             index++) {

            RaftLogEntry entry =
                    raftNode.getLogEntryPublic(index);

            if (entry != null) {
                entries.add(entry);
            }
        }

        AppendEntriesRequest request =
                new AppendEntriesRequest(
                        raftNode.getCurrentTerm(),
                        raftNode.getNodeId(),
                        prevLogIndex,
                        prevLogTerm,
                        entries,
                        raftNode.getCommitIndex()
                );

        try {

            AppendEntriesResponse response =
                    restTemplate.postForObject(
                            peer + "/raft/append",
                            request,
                            AppendEntriesResponse.class
                    );

            if (response == null) {
                return false;
            }

            /*
             * A higher term means we are no longer leader.
             */
            if (response.getTerm()
                    > raftNode.getCurrentTerm()) {

                raftNode.becomeFollower(
                        response.getTerm()
                );

                return false;
            }

            /*
             * Successful replication.
             */
            if (response.isSuccess()) {

                int matchIndex =
                        response.getMatchIndex();

                state.setMatchIndex(
                        matchIndex
                );

                state.setNextIndex(
                        matchIndex + 1
                );

                System.out.println(
                        "📦 "
                                + raftNode.getNodeId()
                                + " replicated "
                                + peer
                                + " through index "
                                + matchIndex
                );

                return true;
            }

            /*
             * Follower rejected because of a log conflict.
             *
             * Jump directly to conflictIndex.
             */
            int conflictIndex =
                    response.getConflictIndex();

            if (conflictIndex > 0) {

                int oldNextIndex =
                        state.getNextIndex();

                state.setNextIndex(
                        Math.max(
                                1,
                                conflictIndex
                        )
                );

                System.out.println(
                        "↩️ Conflict with "
                                + peer
                                + " | nextIndex "
                                + oldNextIndex
                                + " → "
                                + state.getNextIndex()
                );

            } else {

                state.setNextIndex(
                        Math.max(
                                1,
                                state.getNextIndex() - 1
                        )
                );
            }

        } catch (Exception e) {

            System.out.println(
                    "⚠️ Could not contact "
                            + peer
                            + " | "
                            + e.getMessage()
            );

            return false;
        }

        return false;
    }

    /*
     * =========================================================
     * BACKGROUND FOLLOWER CATCH-UP
     * =========================================================
     *
     * Heartbeat scheduler calls this repeatedly.
     *
     * A follower that is behind will receive a batch every
     * cycle until it catches up.
     */
    public synchronized void replicateToAllFollowers() {

        if (raftNode.getRole()
                != RaftState.Role.LEADER) {

            return;
        }

        initializeFollowerState();

        for (String peer : getPeers()) {

            /*
             * One RPC per follower per heartbeat cycle.
             *
             * No recursive backtracking.
             */
            replicateToFollower(peer);
        }

        /*
         * Try to advance commit index using replicated
         * matchIndex values.
         */
        advanceCommitFromMajority();
    }

    /*
     * =========================================================
     * MAJORITY COMMIT CALCULATION
     * =========================================================
     *
     * Find the highest log index replicated on a majority
     * of the cluster.
     */
    private void advanceCommitFromMajority() {

        int currentTerm =
                raftNode.getCurrentTerm();

        int lastIndex =
                raftNode.getLastLogIndex();

        int clusterSize =
                getPeers().size() + 1;

        int majority =
                (clusterSize / 2) + 1;

        for (int index = lastIndex;
             index > raftNode.getCommitIndex();
             index--) {

            RaftLogEntry entry =
                    raftNode.getLogEntryPublic(index);

            if (entry == null) {
                continue;
            }

            /*
             * Raft rule:
             * leader only directly advances commitIndex
             * for an entry from its current term.
             */
            if (entry.getTerm() != currentTerm) {
                continue;
            }

            int replicatedCount = 1;

            for (String peer : getPeers()) {

                RaftReplicationState state =
                        replicationState.get(peer);

                if (state != null
                        && state.getMatchIndex()
                        >= index) {

                    replicatedCount++;
                }
            }

            if (replicatedCount >= majority) {

                raftNode.advanceCommitIndex(
                        index
                );

                System.out.println(
                        "🟢 Majority commit advanced to "
                                + index
                );

                break;
            }
        }
    }

    /*
     * Parse configured peers.
     */
    private List<String> getPeers() {

        List<String> result =
                new ArrayList<>();

        if (peers == null
                || peers.isBlank()) {

            return result;
        }

        for (String peer :
                peers.split(",")) {

            String trimmed =
                    peer.trim();

            if (!trimmed.isEmpty()) {
                result.add(trimmed);
            }
        }

        return result;
    }
}