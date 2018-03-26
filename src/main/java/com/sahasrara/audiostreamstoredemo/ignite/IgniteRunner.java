package com.sahasrara.audiostreamstoredemo.ignite;

import com.sahasrara.audiostreamstoredemo.Runner;
import javazoom.jl.player.Player;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;
import redis.clients.jedis.JedisPool;

import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/**
 * Instructions:
 * 1) Download and start an Apache Ignite Cluster
 *      1.5) Make sure to use the example-ignite.xml -> ./ignite.sh ../config/example-ignite.xml
 * 2) Run main with the IgniteRunner uncommented
 *
 * https://stackoverflow.com/questions/49460554/apache-ignite-streaming-very-slow
 */
public class IgniteRunner implements Runner.DemoRunner {

    private final ExecutorService executorService;
    private final Ignite ignite;

    public IgniteRunner(ExecutorService executorService) {
        this.executorService = executorService;

        // Create the main cache
        ignite = Ignition.start(getResource("example-ignite.xml"));
//        ignite.getOrCreateCache(STREAM_ID).close();
    }

    public void shutdown() {
        ignite.close();
    }

    @Override
    public void streamThenPlayStream(RunInformation measurement) {
        // bla
    }

    @Override
    public void spawnBenchMarker(RunInformation runInformation) {
        long startTime = System.currentTimeMillis();

        // Create Stream ID
        String streamId = String.format(STREAM_ID_PATTERN, runInformation.spawnId);

        // Configure the cache
        CacheConfiguration<Integer, byte[]> cacheConfiguration = new CacheConfiguration();
        cacheConfiguration.setName(streamId);
        cacheConfiguration.setGroupName(STREAM_ID);
        cacheConfiguration.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(Duration.ONE_MINUTE));

        // Create Cache
        ignite.getOrCreateCache(cacheConfiguration).close();

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
        return executorService.submit(() -> {
            long startTime = System.currentTimeMillis();
            try (InputStream musicStream = getResourceAsStream(fileName);
                 IgniteDataStreamer<Integer, byte[]> streamer = ignite.dataStreamer(streamId);
            ) {
                // Configure loader.
                streamer.perNodeBufferSize(CHUNK_SIZE);
                streamer.perNodeParallelOperations(8);
                streamer.allowOverwrite(true);

                // Stream
                int bytesRead;
                byte[] musicChunk = new byte[CHUNK_SIZE];
                int i = 0;
                while ((bytesRead = musicStream.read(musicChunk)) > 0) {
//                    System.out.println("Streaming chunk = " + i);
                    if (bytesRead < musicChunk.length) {
                        byte[] lastChunk = new byte[bytesRead];
                        System.arraycopy(musicChunk, 0, lastChunk, 0, bytesRead);
                        streamer.addData(i++, lastChunk);
                    } else {
                        streamer.addData(i++, musicChunk);
                    }
                    streamer.flush();
                }
            } catch (Exception e) {
                System.out.println("Failure during write" + e.getMessage());
            }
            runInformation.writeTime = System.currentTimeMillis() - startTime;
        });
    }

    private Future readAudio(Ignite ignite, String streamId, RunInformation runInformation) {
        return executorService.submit(() -> {
            long startTime = System.currentTimeMillis();
            try {
                AtomicInteger chunkReadCount = new AtomicInteger(0);
                ContinuousQuery<Integer, byte[]> query = new ContinuousQuery<>();
                query.setInitialQuery(
                        new ScanQuery<>((IgniteBiPredicate<Integer, byte[]>) (integer, bytes) -> integer >= 0));

                // Listener
                query.setLocalListener(iterable -> {
                    for (CacheEntryEvent<? extends Integer, ? extends byte[]> entry : iterable) {
                        try {
//                            System.out.println("Read chunk " + entry.getKey() + " bytes=" + entry.getValue().length + ']');
                            chunkReadCount.incrementAndGet();
                        } catch (Exception e) {
                            System.out.println("Failure during write");
                        }
                    }
                });

                // Execute query.
                try (IgniteCache<Integer, byte[]> cache = ignite.getOrCreateCache(streamId);
                     QueryCursor<Cache.Entry<Integer, byte[]>> cursor = cache.query(query)) {
                    for (Cache.Entry<Integer, byte[]> chunk : cursor) {
                        chunkReadCount.incrementAndGet();
                    }

                    while (chunkReadCount.get() < CHUNK_COUNT) {
                        System.out.println("Sleeping" + chunkReadCount.get());
                        Thread.sleep(100);
                    }
                }
            } catch (Exception e) {
                System.out.println("Failure during read " + e.getMessage());
            }
            runInformation.readTime = System.currentTimeMillis() - startTime;
        });
    }





//    private Future startStreamWriter(Ignite ignite) throws IOException {
//        return executorService.submit((Callable<Void>) () -> {
//            System.out.println("Starting to stream...");
//            try (InputStream musicStream = getResourceAsStream("music.mp3");
//                 IgniteDataStreamer<Integer, byte[]> streamer = ignite.dataStreamer(CACHE_NAME);
//            ) {
//                // Configure loader.
//                streamer.perNodeBufferSize(1024);
//                streamer.perNodeParallelOperations(8);
//                streamer.allowOverwrite(true);
//
//                // Stream
//                int bytesRead;
//                int totalBytes = 0;
//                byte[] musicChunk = new byte[1024];
//                int i = 0;
//                while ((bytesRead = musicStream.read(musicChunk)) > 0) {
//                    System.out.println("Streaming chunk = " + i);
//                    if (bytesRead < musicChunk.length) {
//                        byte[] lastChunk = new byte[bytesRead];
//                        System.arraycopy(musicChunk, 0, lastChunk, 0, bytesRead);
//                        streamer.addData(i++, lastChunk);
//                    } else {
//                        streamer.addData(i++, musicChunk);
//                    }
//                    totalBytes += bytesRead;
//                    streamer.flush();
//                }
//                System.out.println("Bytes streamed = " + totalBytes);
//            } catch (Exception e) {
//                System.out.println("Failure during write" + e.getMessage());
//            }
//            return null;
//        });
//    }
//
//    private PipedOutputStream startStreamReader(IgniteCache<Integer, byte[]> cache) throws Exception {
//        System.out.println("Starting to read...");
//        PipedOutputStream pipedOutputStream = new PipedOutputStream();
//        executorService.submit((Callable<Void>) () -> {
//            try {
//                ContinuousQuery<Integer, byte[]> query = new ContinuousQuery<>();
//                query.setInitialQuery(new ScanQuery<>(new IgniteBiPredicate<Integer, byte[]>() {
//                    @Override
//                    public boolean apply(Integer integer, byte[] bytes) {
//                        return integer >= 0;
//                    }
//                }));
//
//                // Listener
//                final AtomicBoolean closed = new AtomicBoolean(false);
//                OrderedAccumulator<byte[]> accumulator = new OrderedAccumulator<>((index, audio) -> {
//                    try {
//                        System.out.println("Playing " + index);
//                        pipedOutputStream.write(audio);
//                        if (index == 9793) {
//                            pipedOutputStream.close();
//                            closed.set(true);
//                        }
//                    } catch (IOException e) {
//                        System.out.println("Failed to write audio to piped output stream " + e.getMessage());
//                    }
//                });
//
//                query.setLocalListener(new CacheEntryUpdatedListener<Integer, byte[]>() {
//                    @Override
//                    public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends byte[]>> iterable)
//                            throws CacheEntryListenerException {
//                        for (CacheEntryEvent<? extends Integer, ? extends byte[]> entry : iterable) {
//                            System.out.println("Read chunk " + entry.getKey() + " bytes="
//                                    + entry.getValue().length + ']');
//                            try {
//                                // Keys don't necessarily come back to us in order
//                                accumulator.put(entry.getKey(), entry.getValue());
//                            } catch (Exception e) {
//                                System.out.println("Failure during write");
//                            }
//                        }
//                    }
//                });
//
//                // Execute query.
//                try (QueryCursor<Cache.Entry<Integer, byte[]>> cursor = cache.query(query)) {
//                    while (!closed.get()) {
//                        System.out.println("Waiting to exit cursor...");
//                        Thread.sleep(1000);
//                    }
//                }
//            } catch (Exception e) {
//                System.out.println("Failure during read " + e.getMessage());
//            }
//            return null;
//        });
//        return pipedOutputStream;
//    }
//
//    private class OrderedAccumulator<V> {
//        private final ConcurrentSkipListMap<Integer, V> data = new ConcurrentSkipListMap<>();
//        private final BiConsumer<Integer, V> chunkCompleteCallback;
//        private int currentChunk;
//
//        OrderedAccumulator(BiConsumer<Integer, V> chunkCompleteCallback) {
//            this.chunkCompleteCallback = chunkCompleteCallback;
//            this.currentChunk = 0;
//        }
//
//        synchronized void put(Integer key, V value) {
//            data.put(key, value);
//
//            for (Iterator<Map.Entry<Integer, V>> iterator = data.entrySet().iterator(); iterator.hasNext();) {
//                Map.Entry<Integer, V> chunk = iterator.next();
//                if (!(chunk.getKey() == currentChunk)) {
//                    break;
//                }
//                currentChunk++;
//                chunkCompleteCallback.accept(chunk.getKey(), chunk.getValue());
//                iterator.remove();
//            }
//        }
//    }
//
//    public void streamThenPlayStream(RunInformation runInformation) {
//        Future streamerFuture = null;
//        // Configure the cache
//        CacheConfiguration<Integer, byte[]> cacheConfiguration = new CacheConfiguration(STREAM_ID);
//        cacheConfiguration.setGroupName(runInformation.spawnId.toString());
//        cacheConfiguration.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(Duration.ONE_MINUTE));
//        try (Ignite ignite = Ignition.start(getResource("example-ignite.xml"));
//             IgniteCache<Integer, byte[]> cache = ignite.getOrCreateCache(cacheConfiguration)) {
//            // Read Audio
//            PipedOutputStream pipedOutputStream = startStreamReader(cache);
//
//            // Play Audio
//            playAudio(pipedOutputStream);
//
//            // Stream Audio
//            streamerFuture = startStreamWriter(ignite);
//            streamerFuture.get(1, TimeUnit.DAYS);
//        } catch (Exception e) {
//            System.out.println("Failure during ignite demo " + e.getMessage());
//            if (streamerFuture != null) {
//                streamerFuture.cancel(true);
//            }
//        }
//    }
//
//    private void playAudio(PipedOutputStream pipedOutputStream) {
//        executorService.execute(() -> {
//            try {
//                Player playMP3 = new Player(new PipedInputStream(pipedOutputStream, 8192));
//                playMP3.play();
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        });
//    }
}
