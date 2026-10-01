package com.yash.distributed_job_queue.node;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class RaftHeartbeatScheduler {

    private final RaftNode raftNode;
    private final RaftReplicationService replicationService;

    public RaftHeartbeatScheduler(
            RaftNode raftNode,
            RaftReplicationService replicationService) {

        this.raftNode = raftNode;
        this.replicationService = replicationService;
    }

    /*
      Frequent leader communication.
     If followers are behind, the same mechanism also
     performs batch catch-up.
     */
    @Scheduled(fixedRate = 100)
    public void sendHeartbeat() {

        if (raftNode.getRole()
                == RaftState.Role.LEADER) {

            replicationService
                    .replicateToAllFollowers();
        }
    }
}