import { useEffect, useState } from "react";
import {
  Activity,
  CheckCircle2,
  CircleAlert,
  Crown,
  Database,
  Server,
  Send,
  ShieldCheck,
  Wifi,
  WifiOff,
} from "lucide-react";

import {
  findLeader,
  submitJob,
} from "./services/api";

function App() {
  const [nodes, setNodes] = useState([]);
  const [leader, setLeader] = useState(null);
  const [loading, setLoading] = useState(true);

  const [jobId, setJobId] = useState("");
  const [jobType, setJobType] = useState("NORMAL");
  const [payload, setPayload] = useState("");

  const [submitting, setSubmitting] = useState(false);
  const [message, setMessage] = useState("");
  const [messageType, setMessageType] = useState("");

  const [jobs, setJobs] = useState([]);

  
  // Load cluster state

  const loadCluster = async () => {
    try {
      const result = await findLeader();

      setNodes(result.nodes || []);
      setLeader(result.leader || null);
    } catch (error) {
      console.error("Cluster loading failed:", error);
      setNodes([]);
      setLeader(null);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadCluster();

    const interval = setInterval(() => {
      loadCluster();
    }, 3000);

    return () => clearInterval(interval);
  }, []);

 
  // Submit Job
  
  const handleSubmitJob = async (e) => {
    e.preventDefault();

    setMessage("");
    setMessageType("");

    if (!jobId.trim()) {
      setMessage("Please enter a Job ID.");
      setMessageType("error");
      return;
    }

    if (!payload.trim()) {
      setMessage("Please enter a payload.");
      setMessageType("error");
      return;
    }

    if (!leader) {
      setMessage("No Raft leader is currently available.");
      setMessageType("error");
      return;
    }

    setSubmitting(true);

    try {
      const job = {
        jobId: jobId.trim(),
        type: jobType,
        payload: payload.trim(),
      };

      const response = await submitJob(leader.port, job);

      const success = String(response).startsWith("SUCCESS");

      setMessage(response);
      setMessageType(success ? "success" : "error");

      if (success) {
        setJobs((prev) => [
          {
            id: job.jobId,
            type: job.type,
            payload: job.payload,
            leader: leader.id,
            time: new Date().toLocaleTimeString(),
            status: "COMMITTED",
          },
          ...prev,
        ]);

        setJobId("");
        setPayload("");

        await loadCluster();
      }
    } catch (error) {
      console.error("Job submission failed:", error);

      setMessage(
        error?.response?.data ||
          error?.message ||
          "Job submission failed."
      );

      setMessageType("error");

      await loadCluster();
    } finally {
      setSubmitting(false);
    }
  };

  // Cluster statistics
  
  const onlineNodes = nodes.filter((node) => node.online).length;

  const clusterHealthy =
    onlineNodes === 3 && leader !== null;

  const currentTerm =
    leader?.state?.currentTerm ??
    leader?.state?.term ??
    "-";

  const commitIndex =
    leader?.state?.commitIndex ?? "-";

  const lastLogIndex =
    leader?.state?.lastLogIndex ?? "-";

    // UI
  return (
    <div className="min-h-screen bg-slate-950 text-slate-100">

      {/* Header */}
      <header className="border-b border-slate-800 bg-slate-950/90">
        <div className="mx-auto max-w-7xl px-6 py-5">

          <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">

            <div>
              <div className="flex items-center gap-3">
                <div className="rounded-xl bg-blue-600/20 p-2">
                  <Database className="h-7 w-7 text-blue-400" />
                </div>

                <div>
                  <h1 className="text-2xl font-bold tracking-tight">
                    Distributed Job Queue
                  </h1>

                  <p className="text-sm text-slate-400">
                    Raft Consensus Cluster
                  </p>
                </div>
              </div>
            </div>

            {/* Cluster health */}
            <div
              className={`flex items-center gap-2 rounded-full border px-4 py-2 text-sm font-semibold ${
                clusterHealthy
                  ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-400"
                  : "border-amber-500/30 bg-amber-500/10 text-amber-400"
              }`}
            >
              {clusterHealthy ? (
                <CheckCircle2 className="h-4 w-4" />
              ) : (
                <CircleAlert className="h-4 w-4" />
              )}

              {clusterHealthy
                ? "CLUSTER HEALTHY"
                : "CLUSTER DEGRADED"}
            </div>
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-7xl space-y-6 px-6 py-8">

        {/* Current leader banner */}
        <section className="overflow-hidden rounded-2xl border border-slate-800 bg-gradient-to-r from-slate-900 to-slate-950 shadow-xl">

          <div className="flex flex-col gap-6 p-6 lg:flex-row lg:items-center lg:justify-between">

            <div className="flex items-center gap-4">

              <div className="rounded-2xl bg-amber-500/10 p-4">
                <Crown className="h-9 w-9 text-amber-400" />
              </div>

              <div>
                <p className="text-sm font-medium uppercase tracking-wider text-slate-400">
                  Current Raft Leader
                </p>

                <h2 className="mt-1 text-3xl font-bold">
                  {leader
                    ? leader.id.toUpperCase()
                    : "NO LEADER"}
                </h2>

                {leader && (
                  <p className="mt-1 text-sm text-slate-400">
                    Port {leader.port} · Automatically detected
                  </p>
                )}
              </div>
            </div>

            <div className="grid grid-cols-3 gap-3">

              <StatBox
                label="Term"
                value={currentTerm}
              />

              <StatBox
                label="Commit Index"
                value={commitIndex}
              />

              <StatBox
                label="Log Index"
                value={lastLogIndex}
              />

            </div>
          </div>
        </section>

        {/* Cluster overview */}
        <section>

          <div className="mb-4 flex items-center justify-between">

            <div>
              <h2 className="text-xl font-bold">
                Cluster Nodes
              </h2>

              <p className="text-sm text-slate-400">
                Live Raft cluster status
              </p>
            </div>

            <div className="flex items-center gap-2 text-sm text-slate-400">
              <Server className="h-4 w-4" />
              {onlineNodes}/3 Online
            </div>
          </div>

          <div className="grid gap-4 md:grid-cols-3">

            {nodes.map((node) => (
              <NodeCard
                key={node.id}
                node={node}
                isLeader={leader?.id === node.id}
              />
            ))}

            {loading && nodes.length === 0 && (
              <>
                <LoadingCard />
                <LoadingCard />
                <LoadingCard />
              </>
            )}
          </div>
        </section>

        {/* Job submission */}
        <section className="grid gap-6 lg:grid-cols-3">

          {/* Form */}
          <div className="lg:col-span-2">

            <div className="rounded-2xl border border-slate-800 bg-slate-900 p-6 shadow-lg">

              <div className="mb-6 flex items-center gap-3">

                <div className="rounded-xl bg-blue-500/10 p-3">
                  <Send className="h-5 w-5 text-blue-400" />
                </div>

                <div>
                  <h2 className="text-xl font-bold">
                    Submit New Job
                  </h2>

                  <p className="text-sm text-slate-400">
                    Jobs are automatically submitted to the current Raft leader.
                  </p>
                </div>

              </div>

              <form
                onSubmit={handleSubmitJob}
                className="space-y-5"
              >

                <div className="grid gap-5 md:grid-cols-2">

                  <div>
                    <label className="mb-2 block text-sm font-medium text-slate-300">
                      Job ID
                    </label>

                    <input
                      value={jobId}
                      onChange={(e) =>
                        setJobId(e.target.value)
                      }
                      placeholder="job-1001"
                      className="w-full rounded-xl border border-slate-700 bg-slate-950 px-4 py-3 text-sm outline-none transition focus:border-blue-500"
                    />
                  </div>

                  <div>
                    <label className="mb-2 block text-sm font-medium text-slate-300">
                      Job Type
                    </label>

                    <select
                      value={jobType}
                      onChange={(e) =>
                        setJobType(e.target.value)
                      }
                      className="w-full rounded-xl border border-slate-700 bg-slate-950 px-4 py-3 text-sm outline-none focus:border-blue-500"
                    >
                      <option value="NORMAL">
                        NORMAL
                      </option>

                      <option value="FAIL">
                        FAIL
                      </option>
                    </select>
                  </div>

                </div>

                <div>
                  <label className="mb-2 block text-sm font-medium text-slate-300">
                    Payload
                  </label>

                  <textarea
                    value={payload}
                    onChange={(e) =>
                      setPayload(e.target.value)
                    }
                    placeholder="Process this job..."
                    rows={4}
                    className="w-full resize-none rounded-xl border border-slate-700 bg-slate-950 px-4 py-3 text-sm outline-none transition focus:border-blue-500"
                  />
                </div>

                {message && (
                  <div
                    className={`rounded-xl border px-4 py-3 text-sm ${
                      messageType === "success"
                        ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-400"
                        : "border-red-500/30 bg-red-500/10 text-red-400"
                    }`}
                  >
                    {message}
                  </div>
                )}

                <button
                  type="submit"
                  disabled={submitting || !leader}
                  className="flex w-full items-center justify-center gap-2 rounded-xl bg-blue-600 px-5 py-3 font-semibold transition hover:bg-blue-500 disabled:cursor-not-allowed disabled:opacity-40"
                >
                  <Send className="h-4 w-4" />

                  {submitting
                    ? "Submitting..."
                    : leader
                    ? `Submit to ${leader.id}`
                    : "Waiting for Raft Leader"}
                </button>

              </form>
            </div>
          </div>

          {/* System information */}
          <div className="rounded-2xl border border-slate-800 bg-slate-900 p-6 shadow-lg">

            <div className="mb-6 flex items-center gap-3">

              <div className="rounded-xl bg-purple-500/10 p-3">
                <ShieldCheck className="h-5 w-5 text-purple-400" />
              </div>

              <div>
                <h2 className="font-bold">
                  System Status
                </h2>

                <p className="text-xs text-slate-400">
                  Distributed consensus
                </p>
              </div>

            </div>

            <div className="space-y-4">

              <InfoRow
                label="Raft Leader"
                value={
                  leader
                    ? leader.id.toUpperCase()
                    : "Unavailable"
                }
                positive={!!leader}
              />

              <InfoRow
                label="Cluster Nodes"
                value={`${onlineNodes}/3 Online`}
                positive={onlineNodes === 3}
              />

              <InfoRow
                label="Consensus"
                value={
                  leader
                    ? "Operational"
                    : "Waiting"
                }
                positive={!!leader}
              />

              <InfoRow
                label="Auto Refresh"
                value="3 seconds"
                positive
              />

            </div>
          </div>
        </section>

        {/* Job activity */}
        <section className="rounded-2xl border border-slate-800 bg-slate-900 shadow-lg">

          <div className="flex items-center justify-between border-b border-slate-800 p-6">

            <div>
              <h2 className="text-xl font-bold">
                Recent Job Activity
              </h2>

              <p className="text-sm text-slate-400">
                Jobs submitted during this dashboard session
              </p>
            </div>

            <Activity className="h-5 w-5 text-slate-500" />
          </div>

          {jobs.length === 0 ? (
            <div className="p-10 text-center">

              <Database className="mx-auto mb-3 h-8 w-8 text-slate-600" />

              <p className="text-sm text-slate-500">
                No jobs submitted yet.
              </p>

            </div>
          ) : (
            <div className="overflow-x-auto">

              <table className="w-full text-left text-sm">

                <thead className="border-b border-slate-800 text-xs uppercase tracking-wider text-slate-500">

                  <tr>
                    <th className="px-6 py-4">
                      Job ID
                    </th>

                    <th className="px-6 py-4">
                      Type
                    </th>

                    <th className="px-6 py-4">
                      Leader
                    </th>

                    <th className="px-6 py-4">
                      Time
                    </th>

                    <th className="px-6 py-4">
                      Status
                    </th>
                  </tr>

                </thead>

                <tbody>

                  {jobs.map((job, index) => (
                    <tr
                      key={`${job.id}-${index}`}
                      className="border-b border-slate-800/70 last:border-0"
                    >

                      <td className="px-6 py-4 font-medium">
                        {job.id}
                      </td>

                      <td className="px-6 py-4 text-slate-400">
                        {job.type}
                      </td>

                      <td className="px-6 py-4 text-slate-400">
                        {job.leader}
                      </td>

                      <td className="px-6 py-4 text-slate-500">
                        {job.time}
                      </td>

                      <td className="px-6 py-4">

                        <span className="inline-flex items-center gap-1.5 rounded-full bg-emerald-500/10 px-3 py-1 text-xs font-semibold text-emerald-400">

                          <CheckCircle2 className="h-3.5 w-3.5" />

                          {job.status}

                        </span>

                      </td>

                    </tr>
                  ))}

                </tbody>

              </table>

            </div>
          )}
        </section>

      </main>

      <footer className="border-t border-slate-800 py-6 text-center text-xs text-slate-600">
        Distributed Job Queue · Java Spring Boot · Raft Consensus · Redis
      </footer>

    </div>
  );
}

// ----------------------------------
// Components
// ----------------------------------

function StatBox({ label, value }) {
  return (
    <div className="min-w-[100px] rounded-xl border border-slate-800 bg-slate-950/60 px-4 py-3 text-center">
      <p className="text-xs uppercase tracking-wider text-slate-500">
        {label}
      </p>

      <p className="mt-1 text-lg font-bold text-slate-200">
        {value}
      </p>
    </div>
  );
}

function NodeCard({ node, isLeader }) {
  const role =
    node.state?.role?.toUpperCase() ||
    "UNKNOWN";

  const online = node.online;

  const term =
    node.state?.currentTerm ??
    node.state?.term ??
    "-";

  const commitIndex =
    node.state?.commitIndex ?? "-";

  const lastLogIndex =
    node.state?.lastLogIndex ?? "-";

  return (
    <div
      className={`relative overflow-hidden rounded-2xl border bg-slate-900 p-5 shadow-lg transition ${
        isLeader
          ? "border-amber-500/50 shadow-amber-500/5"
          : online
          ? "border-slate-800"
          : "border-red-500/30"
      }`}
    >

      {isLeader && (
        <div className="absolute right-0 top-0 rounded-bl-xl bg-amber-500 px-3 py-1 text-xs font-bold text-slate-950">
          LEADER
        </div>
      )}

      <div className="flex items-start justify-between">

        <div className="flex items-center gap-3">

          <div
            className={`rounded-xl p-3 ${
              isLeader
                ? "bg-amber-500/10"
                : online
                ? "bg-emerald-500/10"
                : "bg-red-500/10"
            }`}
          >
            {isLeader ? (
              <Crown className="h-6 w-6 text-amber-400" />
            ) : (
              <Server
                className={`h-6 w-6 ${
                  online
                    ? "text-emerald-400"
                    : "text-red-400"
                }`}
              />
            )}
          </div>

          <div>
            <h3 className="font-bold">
              {node.id.toUpperCase()}
            </h3>

            <p className="text-xs text-slate-500">
              Port {node.port}
            </p>
          </div>

        </div>

        {online ? (
          <Wifi className="h-5 w-5 text-emerald-400" />
        ) : (
          <WifiOff className="h-5 w-5 text-red-400" />
        )}

      </div>

      <div className="mt-5">

        <div className="flex items-center justify-between">

          <span className="text-xs uppercase tracking-wider text-slate-500">
            Status
          </span>

          <span
            className={`text-sm font-bold ${
              isLeader
                ? "text-amber-400"
                : role === "FOLLOWER"
                ? "text-blue-400"
                : role === "CANDIDATE"
                ? "text-purple-400"
                : "text-slate-400"
            }`}
          >
            {online ? role : "OFFLINE"}
          </span>

        </div>

        <div className="mt-4 grid grid-cols-3 gap-2">

          <MiniMetric
            label="Term"
            value={term}
          />

          <MiniMetric
            label="Commit"
            value={commitIndex}
          />

          <MiniMetric
            label="Log"
            value={lastLogIndex}
          />

        </div>

      </div>
    </div>
  );
}

function MiniMetric({ label, value }) {
  return (
    <div className="rounded-lg bg-slate-950 p-2 text-center">

      <p className="text-[10px] uppercase text-slate-600">
        {label}
      </p>

      <p className="mt-1 text-xs font-semibold text-slate-300">
        {value}
      </p>

    </div>
  );
}

function InfoRow({ label, value, positive }) {
  return (
    <div className="flex items-center justify-between border-b border-slate-800 pb-3 last:border-0">

      <span className="text-sm text-slate-400">
        {label}
      </span>

      <span
        className={`text-sm font-semibold ${
          positive
            ? "text-emerald-400"
            : "text-amber-400"
        }`}
      >
        {value}
      </span>

    </div>
  );
}

function LoadingCard() {
  return (
    <div className="h-52 animate-pulse rounded-2xl border border-slate-800 bg-slate-900" />
  );
}

export default App;