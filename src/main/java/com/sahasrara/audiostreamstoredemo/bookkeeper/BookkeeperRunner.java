package com.sahasrara.audiostreamstoredemo.bookkeeper;

import com.sahasrara.audiostreamstoredemo.Runner;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Instructions:
 * 1) Download and start a Bookkeeper
 * 2) Run main with the BookkeeperRunner uncommented
 */
public class BookkeeperRunner implements Runner.DemoRunner {
    private static final byte[] PASSWORD = "password".getBytes();
    private final ExecutorService executorService;

    public BookkeeperRunner(ExecutorService executorService) {
        this.executorService = executorService;
    }

    @Override
    public void streamThenPlayStream(RunInformation runInformation) {
        // meh
    }

    @Override
    public void spawnBenchMarker(RunInformation runInformation) {
        long startTime = System.currentTimeMillis();
        // Create Stream ID
        Long streamId = null;

        // Create Ledger
        long startLedger = System.currentTimeMillis();
        LedgerHandle ledgerHandle = null;
        BookKeeper bookkeeper = null;
        try {
            bookkeeper = new BookKeeper("127.0.0.1:2181");
            ledgerHandle = bookkeeper.createLedger(1, 1, 1, BookKeeper.DigestType.MAC, PASSWORD);
            streamId = ledgerHandle.getId();
        } catch (Exception e) {
            System.out.println("Failed to create ledger");
        }
        runInformation.writeTime += System.currentTimeMillis() - startLedger;

        // Stream Audio
        Future streamFuture = streamAudio(bookkeeper, ledgerHandle, runInformation.fileName, runInformation);

        // Read Audio
        Future readFuture = readAudio(streamId, runInformation);

        // Wait
        try {
            streamFuture.get(100, TimeUnit.DAYS);
//            readFuture.get(100, TimeUnit.DAYS);
        } catch (Exception e) {
            System.out.println("Task " + runInformation.spawnId + " failed: " + e.getMessage());
        }

        // Record Measurement
        runInformation.timeElasped = System.currentTimeMillis() - startTime;
    }

    private Future readAudio(Long streamId, RunInformation runInformation) {
        return executorService.submit(() -> {
            long startTime = System.currentTimeMillis();
            try (BookKeeper bookkeeper = new BookKeeper("127.0.0.1:2181")) {
                // Open Ledger
                LedgerHandle ledgerHandle = bookkeeper.openLedgerNoRecovery(streamId, BookKeeper.DigestType.MAC, PASSWORD);

                for (int currentChunk = 0; currentChunk < CHUNK_COUNT; ) {
                    byte[] chunk;
                    int entryCount = 0;
                    Enumeration<LedgerEntry> entries = ledgerHandle.readEntries(currentChunk, currentChunk + 10);
                    for (LedgerEntry entry; entries.hasMoreElements(); entryCount++) {
                        entry = entries.nextElement();
                    }
                    currentChunk += entryCount;

                    if (runInformation.firstReadTimestamp == 0) {
                        runInformation.firstReadTimestamp = System.currentTimeMillis();
                    }
                }

                // Delete Ledger
                bookkeeper.deleteLedger(ledgerHandle.getId());
            } catch (Exception e) {
                System.out.println("Failed during read " + e.getMessage());
            }
            runInformation.readTime = System.currentTimeMillis() - startTime;
        });
    }

    private Future streamAudio(BookKeeper keeper, LedgerHandle ledgerHandle, String fileName, RunInformation runInformation) {
        return executorService.submit(() -> {
            long startTime = System.currentTimeMillis();
            try (BookKeeper bookkeeper = keeper;
                 InputStream musicStream = getResourceAsStream(fileName)) {

                int bytesRead;
                byte[] musicChunk = new byte[CHUNK_SIZE];
                runInformation.firstWriteTimestamp = System.currentTimeMillis();
                while ((bytesRead = musicStream.read(musicChunk)) > 0) {
//                    System.out.println("Streaming chunk = " + i++);
                    if (bytesRead < musicChunk.length) {
                        byte[] lastChunk = new byte[bytesRead];
                        System.arraycopy(musicChunk, 0, lastChunk, 0, bytesRead);
                        ledgerHandle.addEntry(lastChunk);
                    } else {
                        ledgerHandle.addEntry(musicChunk);
                    }
                }
                ledgerHandle.close();
            } catch (Exception e) {
                System.out.println("Failed during write " + fileName);
            }
            runInformation.writeTime = System.currentTimeMillis() - startTime;
        });
    }
}
