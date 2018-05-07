package com.o19s.es.ltr.utils;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

@LuceneTestCase.SuppressSysoutChecks(bugUrl = "")
public class AsyncCacheTest extends LuceneTestCase {
    private ExecutorService loaderExec = Executors.newFixedThreadPool(1, factory());
    private ExecutorService sendExec = Executors.newFixedThreadPool(10, factory());

    private AtomicInteger sent = new AtomicInteger();
    private AtomicInteger received = new AtomicInteger();
    private AtomicInteger alreadyAvailable = new AtomicInteger();
    private ConcurrentMap<Integer, AtomicInteger> loaded = new ConcurrentHashMap<>();


    public void testAsyncGet() throws InterruptedException {
        IntStream.range(0, 10).forEach((i) -> loaded.computeIfAbsent(i, (i2) -> new AtomicInteger()));
        ExecutorService executor = Executors.newFixedThreadPool(10, factory());
        AsyncCache<Integer, String, Client> cache = new AsyncCache<Integer, String, Client>(executor, this::load, 1000000, true);

        for(int i = 0; i < 10; i++) {
            sendExec.submit(() -> {
                try {
                    for (int ii = 0; ii < 10000; ii++) {
                        if (true || random().nextBoolean()) {
                            AsyncCacheEntry<String, Client> myloader = cache.asyncGet(random().nextInt(10));
                            int invaId = random().nextInt(10);
                            boolean inva = cache.invalidate(invaId);
                            if (inva) {
                                System.out.println("INVA");
                                loaded.get(invaId).decrementAndGet();
                            }
                        }
                        AsyncCacheEntry<String, Client> myloader = cache.asyncGet(random().nextInt(10));
                        if (myloader.isAvailable()) {
                            alreadyAvailable.incrementAndGet();
                            //sleep(1);

                            //System.out.println(myloader.getClass());
                        } else {
                            //sleep(1);
                            //System.out.println(myloader.getClass());
                            try {
                                accept(myloader);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    System.out.println("SUCCESS");
                }catch (Throwable t) {
                    t.printStackTrace();
                }

            });
        }

        sleep(10000);
        /**
        if (true) {true
        while(true) {
            sleep(10);
            cache.debug();
        }}
        */
        sendExec.shutdown();
        sendExec.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        loaderExec.shutdown();
        loaderExec.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        assertEquals(sent.get(), received.get());
        loaded.values().forEach((i) -> assertEquals("For key: " + i, 1, i.get()));
    }

    private void sleep(long i) {
        try {
            Thread.sleep(i);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void load(Client c, Integer k, ActionListener<String> listener) {
        System.out.println(k);
        loaded.get(k).incrementAndGet();
        System.out.println("MISS");
        loaderExec.submit( () -> {
            //sleep(10);
            listener.onResponse(k + "->HELLO");
        } );
    }

    public void accept(AsyncCacheEntry<String, Client> loader) {
        loader.accept(null, new ActionListener<String>() {
            @Override
            public void onResponse(String s) {
                received.incrementAndGet();
                //System.out.println("H");
            }

            @Override
            public void onFailure(Exception e) {
                e.printStackTrace();
            }
        });
        sent.incrementAndGet();
    }

    public ThreadFactory factory() {
        return new ThreadFactory() {
            @Override
            public Thread newThread(Runnable runnable) {
                Thread t = new Thread(runnable);
                t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread thread, Throwable throwable) {
                        throwable.printStackTrace();
                    }
                });
                return t;
            }
        };
    }
}