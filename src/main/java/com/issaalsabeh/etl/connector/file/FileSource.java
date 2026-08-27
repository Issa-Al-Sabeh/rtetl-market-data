package com.issaalsabeh.etl.connector.file;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.issaalsabeh.etl.core.Source;
import com.issaalsabeh.etl.model.MarketEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileSource implements Source<MarketEvent> {

    private final String filePath;
    private BufferedReader reader;
    private final ObjectMapper objectMapper;
    private static final Logger logger =
            LoggerFactory.getLogger(FileSource.class);

    public FileSource(String filePath) {
        if (filePath == null || filePath.isBlank()) {
            throw new IllegalArgumentException(
                    "File path cannot be null or blank"
            );
        }

        this.filePath = filePath;
        objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();
    }

    @Override
    public void start() {
        try {
            reader = Files.newBufferedReader(
                    Path.of(filePath)
            );
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to open file: " + filePath,
                    e
            );
        }
    }

    @Override
    public MarketEvent poll() {

        if (reader == null) {
            throw new IllegalStateException("Source is not running");
        }

        while (true) {

            String line;

            try {
                line = reader.readLine();

            } catch (IOException e) {
                throw new IllegalStateException(
                        "Failed to read from file: " + filePath,
                        e
                );
            }

            if (line == null) {
                return null;
            }

            try {
                return objectMapper.readValue(
                        line,
                        MarketEvent.class
                );

            } catch (JsonProcessingException e) {
                logger.warn(
                        "Skipping malformed file line: {}",
                        line
                );
            }
        }
    }

    @Override
    public void stop() {

        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                throw new IllegalStateException(
                        "Failed to close file: " + filePath,
                        e
                );
            } finally {
                reader = null;
            }
        }
    }
}
