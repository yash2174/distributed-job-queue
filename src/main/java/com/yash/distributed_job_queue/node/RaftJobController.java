package com.yash.distributed_job_queue.node;

import org.springframework.web.bind.annotation.*;
import org.springframework.web.bind.annotation.CrossOrigin;

@CrossOrigin(origins = "http://localhost:5173")
@RestController
@RequestMapping("/raft/jobs")
public class RaftJobController {

    private final RaftNode raftNode;
    private final RaftReplicationService replicationService;

    public RaftJobController(
            RaftNode raftNode,
            RaftReplicationService replicationService) {

        this.raftNode = raftNode;
        this.replicationService =
                replicationService;
    }

    @PostMapping
    public String submitJob(
            @RequestBody RaftJobCommand command) {

        /*
         * Only leader accepts writes.
         */
        if (raftNode.getRole()
                != RaftState.Role.LEADER) {

            return "REJECTED: This node is not the leader. "
                    + "Current role="
                    + raftNode.getRole();
        }

        try {

            String json =
                    new com.fasterxml.jackson.databind.ObjectMapper()
                            .writeValueAsString(command);

            boolean committed =
                    replicationService.replicate(json);

            if (!committed) {

                return "FAILED: Job was not committed";
            }

            return "SUCCESS: Job committed through Raft"
                    + " | logIndex="
                    + raftNode.getLastLogIndex()
                    + " | term="
                    + raftNode.getCurrentTerm();

        } catch (Exception e) {

            return "FAILED: "
                    + e.getMessage();
        }
    }
}