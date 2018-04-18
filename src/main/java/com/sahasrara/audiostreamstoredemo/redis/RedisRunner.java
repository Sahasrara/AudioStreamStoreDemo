package com.sahasrara.audiostreamstoredemo.redis;

import com.sahasrara.audiostreamstoredemo.Runner;
import javazoom.jl.player.Player;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Instructions:
 * 1) Download and start an Redis
 * 2) Run main with the RedisRunner uncommented
 */
public class RedisRunner implements Runner.DemoRunner {
    private final JedisPool pool;
    private final ExecutorService executorService;

    public RedisRunner(ExecutorService executorService, int runnerCount) {
        this.executorService = executorService;
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(runnerCount * 3);
        jedisPoolConfig.setMaxIdle(runnerCount * 3);
//        jedisPoolConfig.setTestOnBorrow(true);

        this.pool = new JedisPool(jedisPoolConfig, "localhost", 8080);

        Jedis jedis = this.pool.getResource();
        jedis.flushAll();
        jedis.close();
    }

    @Override
    public void streamThenPlayStream(RunInformation runInformation) {
        // Create Stream ID
        String streamId = String.format(STREAM_ID_PATTERN, 0);

        // Stream Audio
        streamAudio(pool, streamId, "music.mp3", runInformation);

        // Read Audio
        InputStream inputStream = readAndReturnAudio(pool, streamId);

        // Play Audio
        try {
            playAudio(inputStream).get(5, TimeUnit.MINUTES);
        } catch (Exception e) {
            System.out.println("Died waiting for audio");
        }
    }

    @Override
    public void spawnBenchMarker(RunInformation runInformation) {
        long startTime = System.currentTimeMillis();
        // Create Stream ID
        String streamId = String.format(STREAM_ID_PATTERN, runInformation.spawnId);

        // Stream Audio
        Future streamFuture = streamAudio_LIST(pool, streamId, runInformation.fileName, runInformation);

        // Read Audio
        Future readFuture = readAudio_LIST(pool, streamId, runInformation);

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

    private Future readAudio_LIST(JedisPool pool, String streamId, RunInformation runInformation) {
        return executorService.submit(() -> {
            long startTime = System.currentTimeMillis();
            try (Jedis jedis = pool.getResource()) {
                for (int currentChunk = 0; currentChunk < CHUNK_COUNT; ) {
                    byte[] chunk;
                    List<byte[]> audioList = jedis.lrange(streamId.getBytes(), currentChunk, -1);
                    while (audioList.size() == 0) {
                        Thread.sleep(50);
                        audioList = jedis.lrange(streamId.getBytes(), currentChunk, -1);
                    }
                    currentChunk += audioList.size();
                    if (runInformation.firstReadTimestamp == 0) {
                        runInformation.firstReadTimestamp = System.currentTimeMillis();
                    }
                }
            } catch (InterruptedException e) {
                System.out.println("Interrupted during polling");
            }
            runInformation.readTime = System.currentTimeMillis() - startTime;
        });
    }

    private Future streamAudio_LIST(JedisPool pool, String streamId, String fileName, RunInformation runInformation) {
        return executorService.submit(() -> {
            long startTime = System.currentTimeMillis();
            try (InputStream musicStream = getResourceAsStream(fileName);
                 Jedis jedis = pool.getResource()) {
                if (jedis.exists(streamId)) {
                    // Delete if exists
                    jedis.del(streamId);
                }

                int bytesRead;
//                int totalBytes = 0;
                byte[] musicChunk = new byte[CHUNK_SIZE];
//                int i = 0;
                runInformation.firstWriteTimestamp = System.currentTimeMillis();
                while ((bytesRead = musicStream.read(musicChunk)) > 0) {
//                    System.out.println("Streaming chunk = " + i++);
                    if (bytesRead < musicChunk.length) {
                        byte[] lastChunk = new byte[bytesRead];
                        System.arraycopy(musicChunk, 0, lastChunk, 0, bytesRead);
                        jedis.rpush(streamId.getBytes(), lastChunk);
                    } else {
                        jedis.rpush(streamId.getBytes(), musicChunk);
                    }
//                    totalBytes += bytesRead;
                }
//                System.out.println("Bytes streamed = " + totalBytes);
            } catch (Exception e) {
                System.out.println("Failed during read " + fileName);
            }
            runInformation.writeTime = System.currentTimeMillis() - startTime;
        });
    }

    private Future readAudio(JedisPool pool, String streamId, RunInformation runInformation) {
        return executorService.submit(() -> {
            long startTime = System.currentTimeMillis();
            try (Jedis jedis = pool.getResource()) {
                for (int currentChunk = 0; currentChunk < CHUNK_COUNT; currentChunk++) {
//                    System.out.println("Fetching chunk " + currentChunk);
                    byte[] chunk;
                    while ((chunk = jedis.get(
                            String.format(UTTERANCE_CHUNK_PATTERN, TEST_UTTERANCE_NAME, currentChunk).getBytes()))
                            == null) {
                        System.out.println("sleeping" + String.format(UTTERANCE_CHUNK_PATTERN, TEST_UTTERANCE_NAME, currentChunk));
                        Thread.sleep(50);
                    }
                    if (runInformation.firstReadTimestamp == 0) {
                        runInformation.firstReadTimestamp = System.currentTimeMillis();
                    }
                }
            } catch (InterruptedException e) {
                System.out.println("Interrupted during polling");
            }
            runInformation.readTime = System.currentTimeMillis() - startTime;
        });
    }

    private Future streamAudio(JedisPool pool, String streamId, String fileName, RunInformation runInformation) {
        return executorService.submit(() -> {
            long startTime = System.currentTimeMillis();
            try (InputStream musicStream = getResourceAsStream(fileName);
                 Jedis jedis = pool.getResource()) {
                int bytesRead;
//                int totalBytes = 0;
                byte[] musicChunk = new byte[CHUNK_SIZE];
                int i = 0;
                runInformation.firstWriteTimestamp = System.currentTimeMillis();
                while ((bytesRead = musicStream.read(musicChunk)) > 0) {
//                    System.out.println("Streaming chunk = " + i);
                    if (bytesRead < musicChunk.length) {
                        byte[] lastChunk = new byte[bytesRead];
                        System.arraycopy(musicChunk, 0, lastChunk, 0, bytesRead);
                        jedis.set(String.format(UTTERANCE_CHUNK_PATTERN, TEST_UTTERANCE_NAME, i).getBytes(), lastChunk);
                    } else {
                        jedis.set(String.format(UTTERANCE_CHUNK_PATTERN, TEST_UTTERANCE_NAME, i).getBytes(), musicChunk);
                    }
//                    totalBytes += bytesRead;
                    i++;
                }
//                System.out.println("Bytes streamed = " + totalBytes);
            } catch (Exception e) {
                System.out.println("Failed during read " + fileName);
            }
            runInformation.writeTime = System.currentTimeMillis() - startTime;
        });
    }

    // Ignore
    private PipedInputStream readAndReturnAudio(JedisPool pool, String streamId) {
        final PipedOutputStream pipedOutputStream = new PipedOutputStream();
        PipedInputStream pipedInputStream = null;
        try {
            pipedInputStream = new PipedInputStream(pipedOutputStream, 65536);
        } catch (IOException e) {
            System.out.println("Couldn't created piped streams");
        }
        executorService.execute(() -> {
            try (Jedis jedis = pool.getResource()) {
                int currentChunk = 0;
                boolean done = false;
                while (!done) {
                    Thread.sleep(50);
                    List<byte[]> audioList = jedis.lrange(streamId.getBytes(), currentChunk, currentChunk + 9);
                    currentChunk += audioList.size();
                    System.out.println("Reading chunks " + audioList.size());
                    for (byte[] audio : audioList) {
                        try {
                            pipedOutputStream.write(audio);
                            if (audio.length < CHUNK_SIZE) {
                                pipedOutputStream.close();
                                done = true;
                            }
                        } catch (IOException e) {
                            System.out.println("Failed to write to piped output stream");
                        }
                    }
                }
            } catch (InterruptedException e) {
                System.out.println("Interrupted during polling");
            }
        });
        return pipedInputStream;
    }
    private Future playAudio(InputStream inputStream) {
        return executorService.submit(() -> {
            try {
                Player playMP3 = new Player(inputStream);
                playMP3.play();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}
