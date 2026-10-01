package com.yash.distributed_job_queue.node;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;

@Component
public class RaftCoordinator {

    private final RaftNode raftNode;
    private final RaftElectionTimer electionTimer;

    private final RestTemplate restTemplate =
            new RestTemplate();

    @Value("${raft.peers}")
    private String peers;

    public RaftCoordinator(
            RaftNode raftNode,
            RaftElectionTimer electionTimer) {

        this.raftNode = raftNode;
        this.electionTimer = electionTimer;
    }

    
     // START ELECTION
     

    public synchronized void startElection() {

        raftNode.startElection();

        int currentTerm =
                raftNode.getCurrentTerm();

        int votes = 1;

        List<String> peerList =
                getPeers();

        int clusterSize =
                peerList.size() + 1;

        int majority =
                (clusterSize / 2) + 1;

        System.out.println(
                "🗳️ "
                        + raftNode.getNodeId()
                        + " requesting votes"
                        + " | term="
                        + currentTerm);

        RequestVoteRequest request =
                new RequestVoteRequest(
                        currentTerm,
                        raftNode.getNodeId(),
                        raftNode.getLastLogIndex(),
                        raftNode.getLastLogTerm());

        for (String peer : peerList) {

            try {

                RequestVoteResponse response =
                        restTemplate.postForObject(
                                peer + "/raft/vote",
                                request,
                                RequestVoteResponse.class);

                if (response == null) {
                    continue;
                }

                /*
                 * We discovered a newer term.
                 */
                if (response.getTerm()
                        > raftNode.getCurrentTerm()) {

                    raftNode.becomeFollower(
                            response.getTerm());

                    electionTimer.reset();

                    return;
                }

                if (response.isVoteGranted()) {

                    votes++;

                    System.out.println(
                            "Vote received from "
                                    + peer);
                }

            } catch (Exception e) {

                System.out.println(
                        "Could not contact "
                                + peer);
            }
        }

        System.out.println(
                "Votes: "
                        + votes
                        + "/"
                        + clusterSize
                        + " | majority="
                        + majority);

        /*
         * Become leader only if we still have
         * the same term and received majority.
         */
        if (votes >= majority
                && raftNode.getCurrentTerm()
                == currentTerm
                && raftNode.getRole()
                == RaftState.Role.CANDIDATE) {

            raftNode.becomeLeader();

            electionTimer.reset();
        }
    }

    
     // HEARTBEATS
     

    public void sendHeartbeats() {

        // Kept for compatibility with existing code.
        // RaftHeartbeatScheduler now uses
        // RaftReplicationService directly.
    }

  
     // PEERS
     

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