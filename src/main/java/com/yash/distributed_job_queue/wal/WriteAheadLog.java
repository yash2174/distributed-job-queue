package com.yash.distributed_job_queue.wal;

import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@Component
public class WriteAheadLog {

    private static final String WAL_FILE = "job-wal.log";

    public void write(String data) {

        try (FileWriter writer = new FileWriter(WAL_FILE, true)) {

            writer.write(data);
            writer.write(System.lineSeparator());

            writer.flush();

        } catch (IOException e) {
            throw new RuntimeException("Failed to write to WAL", e);
        }
    }
    public List<String> readAll() {

    try {
        Path path = Path.of(WAL_FILE);

        if (!Files.exists(path)) {
            return List.of();
        }

        return Files.readAllLines(path);

    } catch (IOException e) {
        throw new RuntimeException("Failed to read WAL", e);
    }
}
}
