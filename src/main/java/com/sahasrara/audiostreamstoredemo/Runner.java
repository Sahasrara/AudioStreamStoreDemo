package com.sahasrara.audiostreamstoredemo;

import com.sahasrara.audiostreamstoredemo.bookkeeper.BookkeeperRunner;
import com.sahasrara.audiostreamstoredemo.ignite.IgniteRunner;
import com.sahasrara.audiostreamstoredemo.memcached.MemcachedRunner;
import com.sahasrara.audiostreamstoredemo.redis.RedisRunner;

import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Runner {
    public static int WAIT_PER_TASK = 100;


    public static void main(String[] args) {
        ExecutorService executorService = Executors.newCachedThreadPool();
        List<DemoRunner.RunInformation> measurements;
        int runnerCount = 1;

        // Ignite
        for (int i = 0; i < 30; i++) {
            measurements = new LinkedList<>();
            IgniteRunner igniteRunner = new IgniteRunner(executorService, runnerCount);
            benchmark(igniteRunner, runnerCount, executorService, measurements);
            igniteRunner.shutdown();
        }

        // Redis
        for (int i = 0; i < 30; i++) {
//            measurements = new LinkedList<>();
//            RedisRunner redisRunner = new RedisRunner(executorService, runnerCount);
//            benchmark(redisRunner, runnerCount, executorService, measurements);
        }

        // Bookkeeper
//        for (int i = 0; i < 30; i++) {
//            measurements = new LinkedList<>();
//            BookkeeperRunner bookkeeperRunner = new BookkeeperRunner(executorService);
//            benchmark(bookkeeperRunner, runnerCount, executorService, measurements);
//        }

        // Memcached Runner
//        MemcachedRunner memcachedRunner = new MemcachedRunner(executorService);
//        benchmark(memcachedRunner, runnerCount, executorService, measurements);
    }

    private static void benchmark(DemoRunner demoRunner, int runnerCount, ExecutorService executorService,
                                  List<DemoRunner.RunInformation> measurements) {
        long startTime = System.currentTimeMillis();
        System.out.println("Spawning " + runnerCount + " runners");
        for (int spawnId = 0; spawnId < runnerCount; spawnId++) {
            DemoRunner.RunInformation runInformation = new DemoRunner.RunInformation(spawnId, "george.wav");
            measurements.add(runInformation);
            runInformation.task = executorService.submit(() -> demoRunner.spawnBenchMarker(runInformation));
        }
        System.out.println("Spawned all runners");

        // Tabulate Results
        System.out.println("Waiting for completion and tabulating results");
        long totalReadTime = 0;
        long totalWriteTime = 0;
        long totalElapsedTime = 0;
        long totalTimeToFirstByte = 0;
        List<Long> timeToFirstByteList = new ArrayList<>(measurements.size());
        for (DemoRunner.RunInformation runInformation : measurements) {
            try {
                runInformation.task.get(WAIT_PER_TASK, TimeUnit.DAYS);
                totalReadTime += runInformation.readTime;
                totalWriteTime += runInformation.writeTime;
                totalElapsedTime += runInformation.timeElasped;
                long timeToFirstByte = runInformation.firstReadTimestamp - runInformation.firstWriteTimestamp;
                totalTimeToFirstByte += timeToFirstByte;
                timeToFirstByteList.add(timeToFirstByte);
            } catch (Exception e) {
                System.out.println("Test task failed: " + e.getMessage());
            }
        }
        long averageReadTime = totalReadTime / runnerCount;
        long averageWriteTime = totalWriteTime / runnerCount;
        long averageElapsedTime = totalElapsedTime / runnerCount;
        long averageTimeToFirstByte = totalTimeToFirstByte / runnerCount;
        long totalTime = System.currentTimeMillis() - startTime;

        // Calculate Percentiles
        timeToFirstByteList.sort(Comparator.naturalOrder());
        long p99 = timeToFirstByteList.get((int) Math.round(0.99 * timeToFirstByteList.size()-1));
        long p90 = timeToFirstByteList.get((int) Math.round(0.90 * timeToFirstByteList.size()-1));
        long p50 = timeToFirstByteList.get((int) Math.round(0.50 * timeToFirstByteList.size()-1));
        System.out.println(
                  "Total Time to stream and read " + runnerCount + " audio files: " + totalTime + "ms.\n"
                + "average complete read and write: " + averageElapsedTime + "ms\n"
                + "average time to first byte read: " + averageTimeToFirstByte + "ms\n"
                + "average read time per runner: " + averageReadTime + "ms\n"
                + "average write time per runner: " + averageWriteTime + "ms\n"
                + "p99 time to first byte: " + p99 + "ms\n"
                + "p90 time to first byte: " + p90 + "ms\n"
                + "p50 time to first byte: " + p50 + "ms\n"
        );
    }

    public interface DemoRunner {
        int CHUNK_SIZE = 4096;
        int TEST_FILE_SIZE = 117832;
        int CHUNK_COUNT = (TEST_FILE_SIZE / CHUNK_SIZE) + ((TEST_FILE_SIZE % CHUNK_SIZE) > 0 ? 1 : 0);
        String UTTERANCE_CHUNK_PATTERN = "utterance-%s-chunk#-%d";
        String TEST_UTTERANCE_NAME = "TEST_UTTERANCE";
        String STREAM_ID = "test";
        String STREAM_ID_PATTERN = STREAM_ID + "-%s";
        void streamThenPlayStream(RunInformation runInformation);
        void spawnBenchMarker(RunInformation runInformation);

        class RunInformation {
            public Future task;
            public final Integer spawnId;
            public long timeElasped;
            public long readTime;
            public long writeTime;
            public long firstWriteTimestamp;
            public long firstReadTimestamp;
            public String fileName;

            RunInformation(int spawnId, String fileName) {
                this.spawnId = spawnId;
                this.fileName = fileName;
                this.timeElasped = 0;
                this.readTime = 0;
                this.writeTime = 0;
                this.firstReadTimestamp = 0;
                this.firstWriteTimestamp = 0;
            }
        }

        default URL getResource(String resource) {
            return Thread.currentThread()
                    .getContextClassLoader()
                    .getResource(resource);
        }

        default InputStream getResourceAsStream(String resource) {
            return Thread.currentThread()
                    .getContextClassLoader()
                    .getResourceAsStream(resource);
        }
    }
}
