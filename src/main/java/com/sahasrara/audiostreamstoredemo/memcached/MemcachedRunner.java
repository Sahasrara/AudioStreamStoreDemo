package com.sahasrara.audiostreamstoredemo.memcached;

import com.sahasrara.audiostreamstoredemo.Runner;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.SerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import redis.clients.jedis.Jedis;

import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Instructions:
 * 1) Download and start an instance of Memcached
 * 2) Run main with the MemcachedRunner uncommented
 */
public class MemcachedRunner implements Runner.DemoRunner {
    private static final int NEVER_EXPIRE = 0;
    private static final Transcoder TRANSCODER = new SerializingTranscoder();
    private final ExecutorService executorService;

    public MemcachedRunner(ExecutorService executorService) {
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
        String streamId = String.format(STREAM_ID_PATTERN, runInformation.spawnId);

        // Create Client
        MemcachedClient memcacheClient;
        try {
            memcacheClient = new MemcachedClient(new InetSocketAddress("localhost", 11211));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create memcached client", e);
        }

        // Stream Audio
        Future streamFuture = streamAudio(memcacheClient, streamId, runInformation.fileName, runInformation);

        // Read Audio
        Future readFuture = readAudio(memcacheClient, streamId, runInformation);

        // Wait
        try {
            streamFuture.get(100, TimeUnit.DAYS);
            readFuture.get(100, TimeUnit.DAYS);
        } catch (Exception e) {
            System.out.println("Task " + runInformation.spawnId + " failed: " + e.getMessage());
        }

        // Record Measurement
        runInformation.timeElasped = System.currentTimeMillis() - startTime;
    }

    private Future streamAudio(MemcachedClient memcacheClient, String streamId, String fileName,
                               RunInformation runInformation) {
        return executorService.submit(() -> {
            long startTime = System.currentTimeMillis();
            try (InputStream musicStream = getResourceAsStream(fileName)) {

                int bytesRead;
//                int totalBytes = 0;
                byte[] musicChunk = new byte[CHUNK_SIZE];
                int i = 0;
                List<Future<Boolean>> results = new LinkedList<>();
                while ((bytesRead = musicStream.read(musicChunk)) > 0) {
//                    System.out.println("Streaming chunk = " + i);
                    if (bytesRead < musicChunk.length) {
                        byte[] lastChunk = new byte[bytesRead];
                        System.arraycopy(musicChunk, 0, lastChunk, 0, bytesRead);
                        results.add(memcacheClient.set(
                                String.format(UTTERANCE_CHUNK_PATTERN, TEST_UTTERANCE_NAME, i), NEVER_EXPIRE,
                                lastChunk, TRANSCODER));
                    } else {
                        results.add(memcacheClient.set(String.format(UTTERANCE_CHUNK_PATTERN, TEST_UTTERANCE_NAME, i), NEVER_EXPIRE,
                                musicChunk, TRANSCODER));
                    }
                    i++;
//                    totalBytes += bytesRead;
                }
                for (Future<Boolean> result : results) {
                    result.get(1, TimeUnit.DAYS);
                }
//                System.out.println("Bytes streamed = " + totalBytes);
            } catch (Exception e) {
                System.out.println("Failed during read " + fileName);
            }
            runInformation.writeTime = System.currentTimeMillis() - startTime;
        });
    }

    private Future readAudio(MemcachedClient memcacheClient, String streamId, RunInformation runInformation) {
        return executorService.submit(() -> {
            long startTime = System.currentTimeMillis();
            try {
                for (int currentChunk = 0; currentChunk < CHUNK_COUNT; currentChunk++) {
//                    System.out.println("Fetching chunk " + currentChunk);
                    byte[] chunk;
                    while ((chunk = (byte[]) memcacheClient.get(
                            String.format(UTTERANCE_CHUNK_PATTERN, TEST_UTTERANCE_NAME, currentChunk), TRANSCODER))
                            == null) {
                        Thread.sleep(50);
                    }
                }
            } catch (InterruptedException e) {
                System.out.println("Interrupted during polling");
            }
            runInformation.readTime = System.currentTimeMillis() - startTime;
        });
    }
}
