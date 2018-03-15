package com.sahasrara.audiostreamstoredemo;

import com.sahasrara.audiostreamstoredemo.ignite.IgniteRunner;

import java.io.InputStream;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Runner {



    public static void main(String[] args) {
        // Ignite
        IgniteRunner igniteRunner = new IgniteRunner();
        igniteRunner.run();
    }

    public interface DemoRunner {
        String CACHE_NAME = "StreamCache";
        ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool();
        void run();
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
