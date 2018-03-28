package com.sahasrara.audiostreamstoredemo.memcached;

import com.sahasrara.audiostreamstoredemo.Runner;
import net.spy.memcached.MemcachedClient;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;

import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Instructions:
 * 1) Download and start an instance of Memcached
 * 2) Run main with the MemcachedRunner uncommented
 */
public class MemcachedRunner implements Runner.DemoRunner {

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
        Future streamFuture = streamAudio(ignite, streamId, runInformation.fileName, runInformation);

        // Read Audio
        Future readFuture = readAudio(ignite, streamId, runInformation);

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

    private Future streamAudio(Ignite ignite, String streamId, String fileName, RunInformation runInformation) {

    }

    private Future readAudio(Ignite ignite, String streamId, RunInformation runInformation) {

    }
}
