package com.yash.distributed_job_queue.node;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class RaftElectionScheduler {

    private final RaftNode raftNode;
    private final RaftCoordinator raftCoordinator;
    private final RaftElectionTimer electionTimer;

    private boolean electionInProgress = false;

    public RaftElectionScheduler(
            RaftNode raftNode,
            RaftCoordinator raftCoordinator,
            RaftElectionTimer electionTimer) {

        this.raftNode = raftNode;
        this.raftCoordinator = raftCoordinator;
        this.electionTimer = electionTimer;
    }

    @Scheduled(fixedRate = 50)
    public synchronized void checkElectionTimeout() {

        /*
         * Leaders don't start elections.
         */
        if (raftNode.getRole()
                == RaftState.Role.LEADER) {

            return;
        }

        /*
         * Prevent overlapping elections.
         */
        if (electionInProgress) {

            return;
        }

        /*
         * Election timeout has not expired.
         */
        if (!electionTimer.expired()) {

            return;
        }

        electionInProgress = true;

        /*
         * Reset immediately so another scheduler
         * cycle doesn't start another election.
         */
        electionTimer.reset();

        System.out.println(
                "⏰ Election timeout for "
                        + raftNode.getNodeId());

        try {

            raftCoordinator.startElection();

        } finally {

            electionInProgress = false;
        }
    }
}