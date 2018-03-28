package com.sahasrara.audiostreamstoredemo;

import com.sahasrara.audiostreamstoredemo.ignite.IgniteRunner;
import com.sahasrara.audiostreamstoredemo.redis.RedisRunner;

import java.io.InputStream;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
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
        List<DemoRunner.RunInformation> measurements = new LinkedList<>();
        int runnerCount = 500;

        // Ignite
        IgniteRunner igniteRunner = new IgniteRunner(executorService);
        benchmark(igniteRunner, runnerCount, executorService, measurements);
        igniteRunner.shutdown();

        // Redis
//        RedisRunner redisRunner = new RedisRunner(executorService);
//        redisBenchmark(redisRunner, runnerCount, executorService, measurements);
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
        int totalReadTime = 0;
        int totalWriteTime = 0;
        for (DemoRunner.RunInformation runInformation : measurements) {
            try {
                runInformation.task.get(WAIT_PER_TASK, TimeUnit.DAYS);
                totalReadTime += runInformation.readTime;
                totalWriteTime += runInformation.writeTime;
            } catch (Exception e) {
                System.out.println("Test task failed: " + e.getMessage());
            }
        }
        long averageReadTime = totalReadTime / runnerCount;
        long averageWriteTime = totalWriteTime / runnerCount;
        long totalTime = System.currentTimeMillis() - startTime;
        System.out.println("Total Time to stream and read " + runnerCount + " audio files: " + totalTime + "ms.\n"
                + "average read time per runner: " + averageReadTime + "ms\n"
                + "average write time per runner: " + averageWriteTime + "ms\n"
        );
    }


    public interface DemoRunner {
        int CHUNK_SIZE = 4096;
        int TEST_FILE_SIZE = 117832;
        int CHUNK_COUNT = TEST_FILE_SIZE / CHUNK_SIZE;
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
            public String fileName;

            RunInformation(int spawnId, String fileName) {
                this.spawnId = spawnId;
                this.fileName = fileName;
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
