package com.yash.distributed_job_queue.node;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@Component
public class RaftPersistentLog {

    private final ObjectMapper objectMapper;

    private final File logFile;

    private final List<RaftLogEntry> entries =
            new ArrayList<>();

    public RaftPersistentLog(
            ObjectMapper objectMapper,
            @Value("${raft.node-id}") String nodeId) {

        this.objectMapper = objectMapper;

        /*
         * Every Raft node gets its own persistent directory.
         *
         * node-1 -> data/raft/node-1/raft-log.json
         * node-2 -> data/raft/node-2/raft-log.json
         * node-3 -> data/raft/node-3/raft-log.json
         */
        File directory =
                new File(
                        "data/raft/"
                                + nodeId);

        if (!directory.exists()
                && !directory.mkdirs()) {

            throw new RuntimeException(
                    "Failed to create Raft storage directory: "
                            + directory.getAbsolutePath());
        }

        this.logFile =
                new File(
                        directory,
                        "raft-log.json");

        load();
    }

    /*
     * ------------------------------------------------
     * APPEND
     * ------------------------------------------------
     */

    public synchronized void append(
            RaftLogEntry entry) {

        /*
         * Prevent accidental duplicate indexes.
         */
        entries.removeIf(
                existing ->
                        existing.getIndex()
                                == entry.getIndex());

        entries.add(entry);

        entries.sort(
                Comparator.comparingInt(
                        RaftLogEntry::getIndex));

        persist();
    }

    /*
     * ------------------------------------------------
     * REPLACE FROM INDEX
     * ------------------------------------------------
     */

    public synchronized void replaceFrom(
            int index,
            List<RaftLogEntry> newEntries) {

        entries.removeIf(
                entry ->
                        entry.getIndex() >= index);

        if (newEntries != null
                && !newEntries.isEmpty()) {

            entries.addAll(newEntries);
        }

        entries.sort(
                Comparator.comparingInt(
                        RaftLogEntry::getIndex));

        persist();
    }

    /*
     * ------------------------------------------------
     * GET ALL
     * ------------------------------------------------
     */

    public synchronized List<RaftLogEntry>
    getAll() {

        return new ArrayList<>(entries);
    }

    /*
     * ------------------------------------------------
     * GET ENTRY
     * ------------------------------------------------
     */

    public synchronized RaftLogEntry
    get(int index) {

        for (RaftLogEntry entry : entries) {

            if (entry.getIndex() == index) {

                return entry;
            }
        }

        return null;
    }

    /*
     * ------------------------------------------------
     * LAST INDEX
     * ------------------------------------------------
     */

    public synchronized int lastIndex() {

        if (entries.isEmpty()) {

            return 0;
        }

        return entries
                .get(entries.size() - 1)
                .getIndex();
    }

    /*
     * ------------------------------------------------
     * CLEAR
     * ------------------------------------------------
     */

    public synchronized void clear() {

        entries.clear();

        persist();
    }

    /*
     * ------------------------------------------------
     * PERSIST
     * ------------------------------------------------
     */

    private void persist() {

        try {

            objectMapper
                    .writerWithDefaultPrettyPrinter()
                    .writeValue(
                            logFile,
                            entries);

        } catch (Exception e) {

            throw new RuntimeException(
                    "Failed to persist Raft log",
                    e);
        }
    }

    /*
     * ------------------------------------------------
     * LOAD
     * ------------------------------------------------
     */

    private void load() {

        try {

            if (!logFile.exists()) {

                return;
            }

            List<RaftLogEntry> loaded =
                    objectMapper.readValue(
                            logFile,
                            new TypeReference<
                                    List<RaftLogEntry>>() {
                            });

            entries.clear();

            if (loaded != null) {

                entries.addAll(loaded);
            }

            entries.sort(
                    Comparator.comparingInt(
                            RaftLogEntry::getIndex));

            System.out.println(
                    "📖 Loaded "
                            + entries.size()
                            + " Raft log entries"
                            + " | file="
                            + logFile.getPath());

        } catch (Exception e) {

            throw new RuntimeException(
                    "Failed to load Raft log",
                    e);
        }
    }
}