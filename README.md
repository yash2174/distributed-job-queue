# Distributed Job Queue

A fault-tolerant distributed job queue built with **Java, Spring Boot, Redis, Raft consensus, and React**.

The project is designed to demonstrate how a distributed job-processing system can maintain a consistent replicated log, elect a leader, replicate jobs across multiple nodes, recover from node failures, and provide a real-time dashboard for monitoring the cluster.

---

## Overview

Traditional job queues often depend on a single server or a centralized message broker. If that server becomes unavailable, job submission and processing can be interrupted.

This project explores a different approach:

- Run multiple queue nodes.
- Use the **Raft consensus algorithm** to coordinate the nodes.
- Elect exactly one node as the leader.
- Send job submissions through the leader.
- Replicate jobs to follower nodes.
- Commit a job after it is replicated to a majority of the cluster.
- Recover followers when they come back online.
- Elect a new leader when the current leader fails.
- Process committed jobs using Redis-backed job processing.
- Monitor the complete cluster through a React dashboard.

The project currently runs as a **three-node Raft cluster**.

![System Architecture](docs/<img width="1536" height="1024" alt="Distributed System Architecture Diagram" src="https://github.com/user-attachments/assets/a3c8aed7-279a-48f1-86ce-8abb89186390" />
)

# Key Features

## Raft Consensus

The backend implements the main mechanisms required for Raft-based cluster coordination:

- Leader election
- Candidate state
- Follower state
- Leader state
- Term management
- Vote requests
- Majority voting
- Leader heartbeats
- Log replication
- Commit index management
- Follower catch-up
- Higher-term detection
- Leader failure recovery
- Old leader recovery

The cluster maintains a single logical leader at a time.

---

## Three-Node Cluster

The default development cluster consists of:

| Node | Port | Default Role |
|------|------|--------------|
| Node-1 | 8080 | Follower/Leader |
| Node-2 | 8081 | Follower/Leader |
| Node-3 | 8082 | Follower/Leader |

The role of each node is dynamic.

For example:


Node-1 → FOLLOWER
Node-2 → LEADER
Node-3 → FOLLOWER


After a leader failure, the cluster can elect another node:

Node-1 → FOLLOWER
Node-2 → OFFLINE
Node-3 → LEADER


The React dashboard detects this automatically.

---

# Why Raft?

A distributed queue needs a mechanism for deciding which node is allowed to make authoritative changes.

Without consensus, two nodes could potentially accept conflicting operations.

Raft provides a structured approach using:

1. Leader election
2. Replicated logs
3. Majority agreement
4. Terms
5. Commit indexes

For a three-node cluster, a majority requires:

```text
2 out of 3 nodes
```

Therefore, a job can be committed when the leader and at least one follower have successfully replicated the entry.

---

# How Job Submission Works

A job is submitted through the React dashboard.

For example:

```json
{
  "jobId": "order-1001",
  "type": "NORMAL",
  "payload": "Process customer order"
}
```

The flow is:
![Job Submission Workflow](docs/<img width="1536" height="1024" alt="Job Submission Workflow Diagram" src="https://github.com/user-attachments/assets/72667baa-1155-47c3-9df0-1a2ebeac7271" />
)

The client does not need to know which node is currently the leader.

The React dashboard automatically discovers the leader and sends the request to the correct node.

---

# Raft Leader Election

Every node starts as a follower.

If a follower does not receive a heartbeat from a leader within its election timeout, it starts an election.

The node:

1. Increments its current term.
2. Changes its role to `CANDIDATE`.
3. Votes for itself.
4. Requests votes from other nodes.
5. Counts the votes.
6. Becomes leader if it receives a majority.

For example:

![Raft Leader Election](docs/<img width="1536" height="1024" alt="Raft Leader Election Process Infographic" src="https://github.com/user-attachments/assets/3c6ba91f-155a-4975-9415-aa33e2257050" />)


The elected leader then begins sending periodic heartbeats.

---

# Leader Heartbeats

The leader periodically communicates with followers.

These heartbeats serve two purposes:

- Maintain leadership
- Replicate pending log entries

The replication scheduler periodically checks whether followers need additional log entries.

This allows a follower that was temporarily unavailable to catch up after returning.

---

# Log Replication

Each Raft node maintains its own persistent Raft log.

The log contains entries such as:


Index    Term    Command
---------------------------------------
1        1       Create Job A
2        1       Create Job B
3        2       Create Job C
4        2       Create Job D


When the leader receives a new job:


Leader Log
     │
     ├── Entry 12404
     ├── Entry 12405
     └── Entry 12406 ← New Job


The leader replicates the entry to followers.

Once the entry has been replicated to a majority, the leader can advance the commit index.

---

# Majority Commit

For a three-node cluster:


Cluster size = 3
Majority = 2


For example:


Node-1 → Leader → Entry 100
Node-2 → Entry 100
Node-3 → Offline


The entry exists on two nodes.

Therefore:


2 / 3 = Majority


The leader can commit the entry.

The dashboard exposes the current:

- Term
- Commit Index
- Last Log Index

so the replication state can be observed visually.

---

# Follower Catch-Up

Suppose Node-3 temporarily goes offline.

While Node-3 is unavailable:


Node-1 → Leader
Node-2 → Follower
Node-3 → Offline


The remaining two nodes can continue operating because they still form a majority.

New jobs can continue to be committed.

When Node-3 comes back:


Node-3 → Follower


The leader compares the follower's log position with its own replication state and sends the missing entries.

Eventually:


Leader:
lastLogIndex = 12406

Node-2:
lastLogIndex = 12406

Node-3:
lastLogIndex = 12406


The recovered follower is synchronized with the cluster.

---

# Leader Failure Recovery

One of the most important failure scenarios is leader failure.

Example:


Before failure:

Node-1 → FOLLOWER
Node-2 → LEADER
Node-3 → FOLLOWER


Node-2 is stopped.

The remaining nodes detect the missing heartbeats.

A new election begins:

Node-1 → CANDIDATE
Node-3 → FOLLOWER / voter


After majority voting:


Node-1 → LEADER
Node-3 → FOLLOWER


The system can then continue accepting jobs through the new leader.

When the old Node-2 is restarted, it does not automatically reclaim leadership.

Instead, it rejoins the cluster and operates as a follower after receiving the current cluster state.

---

# Persistent Raft State

Important Raft state is persisted on disk.

The project maintains per-node state under:


data/
└── raft/
    ├── node-1/
    │   ├── raft-log.json
    │   └── raft-metadata.json
    │
    ├── node-2/
    │   ├── raft-log.json
    │   └── raft-metadata.json
    │
    └── node-3/
        ├── raft-log.json
        └── raft-metadata.json

Persistent metadata includes information such as:

- Current term
- Voted-for node
- Commit index
- Last applied index

The Raft log stores replicated commands.


# Redis Job Processing

Redis is used as part of the job-processing layer.

The system maintains job information such as:


Job ID
Job Type
Payload
Status
Retry Count


The worker processes queued jobs and updates their status.

A simplified lifecycle is:


QUEUED
   │
   ▼
PROCESSING
   │
   ├──────────────► COMPLETED
   │
   └──────────────► FAILED
                         │
                         ▼
                       RETRY
                         │
                         ▼
                    MAX RETRIES
                         │
                         ▼
                        DLQ


The system also supports retry behavior and a dead-letter queue for jobs that repeatedly fail.



# React Monitoring Dashboard

The frontend is built with:

- React
- Vite
- Tailwind CSS
- Axios
- Lucide React

The dashboard provides a visual representation of the distributed system.

It automatically displays:

### Cluster Health


CLUSTER HEALTHY


or:

CLUSTER DEGRADED


### Current Leader

The dashboard automatically detects and displays:


CURRENT RAFT LEADER

NODE-2
Port 8081


### Node Status

Each node displays:

- Online/offline status
- Leader/follower/candidate role
- Current term
- Commit index
- Last log index

Example:


NODE-1
FOLLOWER
Term: 1685
Commit: 12406
Log: 12406



NODE-2
LEADER
Term: 1685
Commit: 12406
Log: 12406



NODE-3
FOLLOWER
Term: 1685
Commit: 12406
Log: 12406


The frontend automatically refreshes cluster information every few seconds.



# Job Submission Dashboard

Users can submit jobs directly from the React interface.

Fields include:

- Job ID
- Job Type
- Payload

The submit button automatically targets the currently elected Raft leader.

For example:


Submit to node-2


If leadership changes:


Submit to node-3


The user does not need to manually change the node or port.



# Technology Stack

## Backend

- Java 21
- Spring Boot
- Spring Web
- Spring Data Redis
- Jackson
- Maven

## Distributed Systems

- Raft consensus
- Leader election
- Majority voting
- Replicated log
- Commit index
- Persistent node state
- Follower recovery
- Leader failover

## Storage / Processing

- Redis
- Local persistent Raft state
- JSON-based persistent log

## Frontend

- React
- Vite
- Tailwind CSS
- Axios
- Lucide React

## Development Tools

- VS Code
- Git
- GitHub
- Thunder Client
- PowerShell
- WSL2

---

# Project Structure


distributed-job-queue/
│
├── distributed-job-queue/
│   │
│   ├── src/
│   │   ├── main/
│   │   │   └── java/
│   │   │       └── com/
│   │   │           └── yash/
│   │   │               └── distributed_job_queue/
│   │   │
│   │   ├── main/
│   │   │   └── resources/
│   │   │
│   │   └── test/
│   │
│   ├── pom.xml
│   └── ...
│
├── distributed-job-queue-ui/
│   │
│   ├── src/
│   │   ├── App.jsx
│   │   ├── main.jsx
│   │   ├── index.css
│   │   └── services/
│   │       └── api.js
│   │
│   ├── package.json
│   ├── vite.config.js
│   └── ...
│
├── .gitignore
└── README.md


---

# Backend Configuration

The backend uses the following default configuration:

yaml
server:
  port: ${SERVER_PORT:8080}

spring:
  data:
    redis:
      host: localhost
      port: 6379

raft:
  node-id: ${RAFT_NODE_ID:node-1}
  peers: ${RAFT_PEERS:http://localhost:8081,http://localhost:8082}


The port and node ID can be overridden through environment variables.

This allows the same Spring Boot application to run as different cluster nodes.



# Prerequisites

Before running the project, install:

- Java 21
- Maven or Maven Wrapper
- Node.js
- npm
- Redis
- Git

Recommended environment:


Java 21
Spring Boot
Node.js
Redis
Git


---

# Running Redis

The project uses Redis locally.

If Redis is running through WSL:

bash
redis-server


Verify Redis:

bash
redis-cli ping


Expected:


PONG


Redis must be running before starting the application nodes.

---

# Running the Backend Cluster

Open three separate terminals.

## Node 1

From the backend directory:

powershell
$env:SERVER_PORT="8080"
$env:RAFT_NODE_ID="node-1"
.\mvnw.cmd spring-boot:run


---

## Node 2

powershell
$env:SERVER_PORT="8081"
$env:RAFT_NODE_ID="node-2"
.\mvnw.cmd spring-boot:run


---

## Node 3

powershell
$env:SERVER_PORT="8082"
$env:RAFT_NODE_ID="node-3"
.\mvnw.cmd spring-boot:run


Wait for the nodes to start and complete leader election.

One node should eventually become:


LEADER


while the others become:


FOLLOWER


---

# Checking Raft State

Each node exposes:

GET /raft/state


For example:


http://localhost:8080/raft/state


or:


http://localhost:8081/raft/state


or:


http://localhost:8082/raft/state


Example response:


node=node-2 | role=LEADER | term=1685 | commitIndex=12406 | lastLogIndex=12406


This endpoint is useful for debugging and validating cluster state.

---

# Running the React Dashboard

Open another terminal.

Navigate to:

cd distributed-job-queue-ui


Install dependencies:

powershell
npm.cmd install


Start the development server:
powershell
npm.cmd run dev


Open:

text
http://localhost:5173


The dashboard will automatically query all three nodes.

---

# Submitting a Job

From the dashboard:

1. Enter a Job ID.
2. Select the job type.
3. Enter the payload.
4. Click **Submit Job**.

The frontend finds the current leader automatically.

For example:


Current Leader: NODE-2

Submit to node-2


The backend then replicates the job through the Raft cluster.

---

# API Endpoints

## Get Raft State

http
GET /raft/state


Returns the current node's Raft state.

---

## Submit Job

http
POST /raft/jobs


Example:

json
{
  "jobId": "job-1001",
  "type": "NORMAL",
  "payload": "Process this job"
}


A successful submission returns a message similar to:


SUCCESS: Job committed through Raft


---

# Testing the Distributed System

The project was tested through several failure scenarios.

## Test 1 — Normal Job Submission

Start all three nodes.

Confirm that one node becomes leader.

Submit jobs through the API or dashboard.

Expected behavior:


Job submitted
      ↓
Leader receives job
      ↓
Followers replicate job
      ↓
Majority reached
      ↓
Commit index advances
      ↓
Job committed

---

# Test 2 — Follower Failure

Stop one follower.

Example:


Node-1 → LEADER
Node-2 → FOLLOWER
Node-3 → OFFLINE


The remaining leader and follower still form a majority.

Submit new jobs.

The cluster should continue accepting jobs.

---

# Test 3 — Follower Recovery

Restart the stopped follower.

The recovered node should:

1. Rejoin the cluster.
2. Become a follower.
3. Receive missing log entries.
4. Catch up with the leader.

The node's log index should eventually match the leader.

---

# Test 4 — Leader Failure

Stop the current leader.

Example:

Before:

Node-1 → FOLLOWER
Node-2 → LEADER
Node-3 → FOLLOWER


Stop Node-2.

The remaining nodes should start an election.

After successful election:

Node-1 → LEADER
Node-2 → OFFLINE
Node-3 → FOLLOWER

The React dashboard should automatically display the new leader.

---

# Test 5 — Old Leader Recovery

Restart the previously failed leader.

It should rejoin the cluster as a follower and synchronize with the current leader.

This verifies that a previously failed node can return without manually restoring the cluster.

---

# Load Test

A baseline load test was performed with:

5,000 jobs


Observed result:


Submitted : 5000
Success   : 5000
Failed    : 0
Duration  : 00:10:45.9640785


This demonstrates successful API-level processing of the 5,000-job baseline under the tested local development environment.

The result should be interpreted as a development/load-test measurement rather than a production performance benchmark.

---

# Failure Recovery Model

The overall recovery architecture can be summarized as:


![Complete Failure Recovery](docs/<img width="1536" height="1024" alt="Complete Failure Recovery Architecture" src="https://github.com/user-attachments/assets/4fdb85fe-ad97-4dfc-954f-79fc1ca0832a" />
)

# Design Decisions

## Why three nodes?

Three nodes provide a simple environment for demonstrating majority-based consensus.


3 nodes
2 required for majority
1 node can fail


This is useful for demonstrating leader failure and follower recovery without requiring a large cluster.

---

## Why Spring Boot?

Spring Boot provides:

- HTTP APIs
- Dependency injection
- Scheduling
- Redis integration
- Production-oriented application structure
- Easy configuration
- Strong Java ecosystem support

It also makes the distributed-system implementation easier to organize into independent components.

---

## Why Redis?

Redis is used as the fast job-processing data layer.

It provides:

- Fast in-memory operations
- Queue-oriented data structures
- Simple integration with Spring Boot
- Useful primitives for distributed job processing

---

## Why React?

The distributed system is difficult to understand by looking only at terminal logs.

The React dashboard makes cluster behavior visible:

Leader
Followers
Offline nodes
Term
Commit index
Log index
Job submissions
Cluster health


This makes it easier to observe leader election and failure recovery.

---

# Reliability Model

The project separates two important concepts:

### Consensus

Raft determines which replicated commands are committed by the cluster.

### Job Processing

The job-processing layer takes committed jobs and processes them asynchronously.

This separation is important because consensus and execution are different responsibilities.

---

# Current Scope

The current implementation demonstrates:

- Three-node distributed cluster
- Raft leader election
- Candidate/follower/leader states
- Majority voting
- Heartbeats
- Replicated logs
- Persistent Raft metadata
- Follower catch-up
- Leader failure recovery
- Old leader recovery
- Redis-backed job processing
- Retry behavior
- Dead-letter queue handling
- React cluster monitoring
- Automatic leader discovery
- Job submission from the dashboard
- 5,000-job baseline load testing

---

# Production Hardening Roadmap

This project is primarily an advanced distributed-systems implementation and portfolio project. Additional engineering would be required before using it as a production job queue.

Potential production hardening includes:

- True append-only WAL implementation
- Log compaction and snapshots
- Stronger state-machine idempotency
- Durable job-state storage
- Duplicate job protection / idempotency keys
- Authentication and authorization
- TLS between cluster nodes
- Secure Redis configuration
- Metrics and monitoring
- Distributed tracing
- Structured logging
- Health and readiness endpoints
- Backpressure and rate limiting
- Resource isolation
- Better retry and backoff policies
- Automated integration and chaos tests
- Containerized deployment
- Kubernetes deployment
- Persistent volume management
- Horizontal scaling
- Operational alerting

These improvements would be required for a production-grade distributed messaging platform.

---

# What This Project Demonstrates

This project was built to go beyond a standard CRUD application.

It demonstrates practical understanding of:

- Distributed systems
- Consensus algorithms
- Leader election
- Replicated state
- Failure detection
- Fault recovery
- Persistence
- Concurrent processing
- Redis
- Spring Boot
- REST APIs
- React
- System monitoring
- Load testing

The main objective was to understand how a distributed system behaves when individual components fail and recover.

---

# Future Improvements

Planned improvements include:

1. Implement a true append-only WAL.
2. Add snapshotting and log compaction.
3. Improve job idempotency.
4. Add persistent job-status APIs.
5. Add authentication and TLS.
6. Add Prometheus-compatible metrics.
7. Add distributed tracing.
8. Add automated chaos testing.
9. Containerize the three-node cluster.
10. Add Kubernetes deployment manifests.
11. Improve horizontal scaling.
12. Add persistent frontend job history.
13. Add detailed cluster event history.
14. Add automated CI/CD with GitHub Actions.

---

# Author

Yash Paithane

Computer Science & Engineering

Built as an advanced distributed-systems project using Java, Spring Boot, Raft, Redis, and React.

---

# License

This project is available for educational and portfolio purposes.

Add an appropriate open-source license if you intend to permit reuse, modification, or redistribution.
