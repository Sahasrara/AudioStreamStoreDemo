package com.sahasrara.audiostreamstoredemo.redis;

import com.sahasrara.audiostreamstoredemo.Runner;
import javazoom.jl.player.Player;
import jdk.internal.util.xml.impl.Input;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by eric on 3/18/18.
 */
public class RedisRunner implements Runner.DemoRunner {
    private static final String STREAM_ID = "test";
    private static final int CHUNK_SIZE = 4096;

    @Override
    public void run() {
        // Create Client
        JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost");

        // Stream Audio
        streamAudio(pool);

        // Read Audio
        InputStream inputStream = readAudio(pool);

        // Play Audio
        try {
            playAudio(inputStream).get(5, TimeUnit.MINUTES);
        } catch (Exception e) {
            System.out.println("Died waiting for audio");
        }
    }

    private Future playAudio(InputStream inputStream) {
        return EXECUTOR_SERVICE.submit(() -> {
            try {
                Player playMP3 = new Player(inputStream);
                playMP3.play();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private PipedInputStream readAudio(JedisPool pool) {
        final PipedOutputStream pipedOutputStream = new PipedOutputStream();
        PipedInputStream pipedInputStream = null;
        try {
            pipedInputStream = new PipedInputStream(pipedOutputStream, 65536);
        } catch (IOException e) {
            System.out.println("Couldn't created piped streams");
        }
        EXECUTOR_SERVICE.execute(() -> {
            try (Jedis jedis = pool.getResource()) {
                int currentChunk = 0;
                boolean done = false;
                while (!done) {
                    Thread.sleep(50);
                    List<byte[]> audioList = jedis.lrange(STREAM_ID.getBytes(), currentChunk, currentChunk + 9);
                    currentChunk += audioList.size();
                    System.out.println("Reading chunks " + audioList.size());
                    for (byte[] audio : audioList) {
                        try {
                            pipedOutputStream.write(audio);
                            if (audio.length < CHUNK_SIZE) {
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

    private void streamAudio(JedisPool pool) {
        EXECUTOR_SERVICE.execute(() -> {
            try (InputStream musicStream = getResourceAsStream("music.mp3");
                 Jedis jedis = pool.getResource()) {
                if (jedis.exists(STREAM_ID)) {
                    // Delete if exists
                    jedis.del(STREAM_ID);
                }

                int bytesRead;
                int totalBytes = 0;
                byte[] musicChunk = new byte[CHUNK_SIZE];
                int i = 0;
                while ((bytesRead = musicStream.read(musicChunk)) > 0) {
                    System.out.println("Streaming chunk = " + i++);
                    Thread.sleep(50);
                    if (bytesRead < musicChunk.length) {
                        byte[] lastChunk = new byte[bytesRead];
                        System.arraycopy(musicChunk, 0, lastChunk, 0, bytesRead);
                        jedis.rpush(STREAM_ID.getBytes(), lastChunk);
                    } else {
                        jedis.rpush(STREAM_ID.getBytes(), musicChunk);
                    }
                    totalBytes += bytesRead;
                }
                System.out.println("Bytes streamed = " + totalBytes);
            } catch (Exception e) {
                System.out.println("Failed during read music.mp3");
            }
        });
    }
}
