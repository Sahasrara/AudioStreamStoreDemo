package com.sahasrara.audiostreamstoredemo.ignite;

import com.sahasrara.audiostreamstoredemo.Runner;
import javafx.scene.media.Media;
import javafx.scene.media.MediaPlayer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;

import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import java.io.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Instructions:
 * 1) Download and start an Apache Ignite Cluster
 *      1.5) Make sure to use the example-ignite.xml -> ./ignite.sh ../config/example-ignite.xml
 * 2) Run main with the IgniteRunner uncommented
 */
public class IgniteRunner implements Runner.DemoRunner {

    public void run() {
        Future streamerFuture = null;
        // Configure the cache
        CacheConfiguration<Integer, byte[]> cacheConfiguration = new CacheConfiguration(CACHE_NAME);
        cacheConfiguration.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(Duration.ONE_MINUTE));
        try (Ignite ignite = Ignition.start(getResource("example-ignite.xml"));
             IgniteCache<Integer, byte[]> cache = ignite.getOrCreateCache(cacheConfiguration);) {
            // Read Audio
            File musicStreamed = startStreamReader(cache);

            // Stream Audio
            streamerFuture = startStreamWriter(ignite, cache);
            streamerFuture.get(1, TimeUnit.DAYS);

            // Play Audio
            new javafx.embed.swing.JFXPanel();
            MediaPlayer player = new MediaPlayer(new Media(musicStreamed.toURI().toString()));
            player.play(); // or stop() or pause() etc etc

        } catch (Exception e) {
            System.out.println("Failure during ignite demo " + e.getMessage());
            if (streamerFuture != null) {
                streamerFuture.cancel(true);
            }
        }
    }

    private Future startStreamWriter(Ignite ignite, IgniteCache<Integer, byte[]> cache) throws IOException {
        return EXECUTOR_SERVICE.submit((Callable<Void>) () -> {
            System.out.println("Starting to stream...");
            try (InputStream musicStream = getResourceAsStream("music.mp3");
                 IgniteDataStreamer<Integer, byte[]> streamer = ignite.dataStreamer(CACHE_NAME);
            ) {
                // Configure loader.
                streamer.perNodeBufferSize(1024);
                streamer.perNodeParallelOperations(8);

                // Stream
                int bytesRead;
                int totalBytes = 0;
                byte[] musicChunk = new byte[1024];
                int i = 0;
                while ((bytesRead = musicStream.read(musicChunk)) > 0) {
                    System.out.println("Streaming chunk = " + i++);
                    streamer.addData(i, musicChunk);
                    totalBytes += bytesRead;
                    streamer.flush();
                }
                System.out.println("Bytes streamed = " + totalBytes);
            } catch (Exception e) {
                System.out.println("Failure during write" + e.getMessage());
            }
            return null;
        });
    }

    private File startStreamReader(IgniteCache<Integer, byte[]> cache) throws Exception {
        System.out.println("Starting to read...");
        File musicStreamed = File.createTempFile("temp",".mp3");
        System.out.println("Temp file = " + musicStreamed.getAbsolutePath());
        musicStreamed.deleteOnExit();
        EXECUTOR_SERVICE.submit((Callable<Void>) () -> {
            try (OutputStream musicStream = new FileOutputStream(musicStreamed)) {
                ContinuousQuery<Integer, byte[]> query = new ContinuousQuery<>();
                query.setInitialQuery(new ScanQuery<>(new IgniteBiPredicate<Integer, byte[]>() {
                    @Override
                    public boolean apply(Integer integer, byte[] bytes) {
                        return integer >= 0;
                    }
                }));

                // Listener
                final AtomicBoolean closed = new AtomicBoolean(false);
                query.setLocalListener(new CacheEntryUpdatedListener<Integer, byte[]>() {
                    @Override
                    public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends byte[]>> iterable)
                            throws CacheEntryListenerException {
                        for (CacheEntryEvent<? extends Integer, ? extends byte[]> entry : iterable) {
                            System.out.println("Read chunk " + entry.getKey() + " bytes="
                                    + entry.getValue().length + ']');
                            try {
                                musicStream.write(entry.getValue());
                                if (entry.getKey() == 9793) {
                                    musicStream.close();
                                    closed.set(true);
                                }
                            } catch (IOException e1) {
                                System.out.println("Failure during write");
                            }
                        }
                    }
                });

                // Execute query.
                try (QueryCursor<Cache.Entry<Integer, byte[]>> cursor = cache.query(query)) {
                    while (!closed.get()) {
                        System.out.println("Waiting to exit cursor...");
                        Thread.sleep(1000);
                    }
                }
            } catch (Exception e) {
                System.out.println("Failure during read " + e.getMessage());
            }
            return null;
        });
        return musicStreamed;
    }
}
