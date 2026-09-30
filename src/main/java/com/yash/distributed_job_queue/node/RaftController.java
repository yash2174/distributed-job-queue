package com.yash.distributed_job_queue.node;

import org.springframework.web.bind.annotation.*;

@CrossOrigin(origins = "http://localhost:5173")
@RestController
@RequestMapping("/raft")
public class RaftController {

    private final RaftNode raftNode;
    private final RaftElectionTimer electionTimer;

    public RaftController(
            RaftNode raftNode,
            RaftElectionTimer electionTimer) {

        this.raftNode = raftNode;
        this.electionTimer = electionTimer;
    }

    @PostMapping("/vote")
    public RequestVoteResponse vote(
            @RequestBody RequestVoteRequest request) {

        RequestVoteResponse response =
                raftNode.handleRequestVote(request);

        if (response.isVoteGranted()) {

            electionTimer.reset();
        }

        return response;
    }

    @PostMapping("/append")
    public AppendEntriesResponse append(
            @RequestBody AppendEntriesRequest request) {

        AppendEntriesResponse response =
                raftNode.handleAppendEntries(request);

        if (response.isSuccess()
                && request.getTerm()
                >= raftNode.getCurrentTerm()) {

            electionTimer.reset();
        }

        return response;
    }

    @GetMapping("/state")
    public String state() {

        return "node="
                + raftNode.getNodeId()
                + " | role="
                + raftNode.getRole()
                + " | term="
                + raftNode.getCurrentTerm()
                + " | commitIndex="
                + raftNode.getCommitIndex()
                + " | lastLogIndex="
                + raftNode.getLastLogIndex();
    }
}