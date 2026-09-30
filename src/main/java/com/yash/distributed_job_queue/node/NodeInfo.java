package com.yash.distributed_job_queue.node;

public class NodeInfo {

    private String nodeId;
    private String host;
    private int port;

    public NodeInfo(String nodeId, String host, int port) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}