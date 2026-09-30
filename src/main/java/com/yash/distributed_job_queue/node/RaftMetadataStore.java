package com.yash.distributed_job_queue.node;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;

@Component
public class RaftMetadataStore {

    private final ObjectMapper objectMapper;

    private final File metadataFile;

    public RaftMetadataStore(
            ObjectMapper objectMapper,
            @Value("${raft.node-id}") String nodeId) {

        this.objectMapper = objectMapper;

        /*
         * Every Raft node gets its own metadata file.
         *
         * node-1 -> data/raft/node-1/raft-metadata.json
         * node-2 -> data/raft/node-2/raft-metadata.json
         * node-3 -> data/raft/node-3/raft-metadata.json
         */
        File directory =
                new File(
                        "data/raft/"
                                + nodeId);

        if (!directory.exists()
                && !directory.mkdirs()) {

            throw new RuntimeException(
                    "Failed to create Raft metadata directory: "
                            + directory.getAbsolutePath());
        }

        this.metadataFile =
                new File(
                        directory,
                        "raft-metadata.json");
    }

    /*
     * ------------------------------------------------
     * LOAD
     * ------------------------------------------------
     */

    public synchronized RaftMetadata load() {

        try {

            if (!metadataFile.exists()) {

                System.out.println(
                        "🆕 No Raft metadata found for node"
                                + " | file="
                                + metadataFile.getPath());

                return new RaftMetadata();
            }

            RaftMetadata metadata =
                    objectMapper.readValue(
                            metadataFile,
                            RaftMetadata.class);

            System.out.println(
                    "📖 Loaded Raft metadata"
                            + " | term="
                            + metadata.getCurrentTerm()
                            + " | votedFor="
                            + metadata.getVotedFor()
                            + " | commit="
                            + metadata.getCommitIndex()
                            + " | applied="
                            + metadata.getLastApplied());

            return metadata;

        } catch (Exception e) {

            throw new RuntimeException(
                    "Failed to load Raft metadata",
                    e);
        }
    }

    /*
     * ------------------------------------------------
     * SAVE
     * ------------------------------------------------
     */

    public synchronized void save(
            RaftMetadata metadata) {

        try {

            objectMapper
                    .writerWithDefaultPrettyPrinter()
                    .writeValue(
                            metadataFile,
                            metadata);

        } catch (Exception e) {

            throw new RuntimeException(
                    "Failed to persist Raft metadata",
                    e);
        }
    }
}