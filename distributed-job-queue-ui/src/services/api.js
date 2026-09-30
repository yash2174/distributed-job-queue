import axios from "axios";

const api = axios.create({
  timeout: 5000,
});

export const getNodeState = async (port) => {
  const response = await api.get(`http://localhost:${port}/raft/state`);

  const text = response.data;

  const match = text.match(
    /node=(.*?)\s*\|\s*role=(.*?)\s*\|\s*term=(\d+)\s*\|\s*commitIndex=(\d+)\s*\|\s*lastLogIndex=(\d+)/
  );

  if (!match) {
    throw new Error(`Invalid Raft state response: ${text}`);
  }

  return {
    nodeId: match[1].trim(),
    role: match[2].trim(),
    currentTerm: Number(match[3]),
    commitIndex: Number(match[4]),
    lastLogIndex: Number(match[5]),
  };
};

export const getClusterStates = async () => {
  const nodes = [
    {
      id: "node-1",
      port: 8080,
    },
    {
      id: "node-2",
      port: 8081,
    },
    {
      id: "node-3",
      port: 8082,
    },
  ];

  const results = await Promise.all(
    nodes.map(async (node) => {
      try {
        const state = await getNodeState(node.port);

        return {
          ...node,
          state,
          online: true,
        };
      } catch {
        return {
          ...node,
          state: null,
          online: false,
        };
      }
    })
  );

  return results;
};

export const findLeader = async () => {
  const nodes = await getClusterStates();

  const leader = nodes.find(
    (node) =>
      node.online &&
      node.state?.role === "LEADER"
  );

  return {
    leader,
    nodes,
  };
};

export const submitJob = async (
  port,
  job
) => {
  const response = await api.post(
    `http://localhost:${port}/raft/jobs`,
    job
  );

  return response.data;
};