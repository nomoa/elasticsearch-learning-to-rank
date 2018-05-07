package com.o19s.es.ltr;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ReleasableLock;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class CacheTest extends LuceneTestCase {
    public void testElas() throws InterruptedException {
        org.elasticsearch.common.cache.Cache<String, String> cache = org.elasticsearch.common.cache.CacheBuilder.<String, String>builder().setExpireAfterAccess(TimeValue.parseTimeValue("5ms", "test"))
                .setExpireAfterWrite(TimeValue.parseTimeValue("5ms", "test")).build();
        ExecutorService service = Executors.newFixedThreadPool(10);
        Runnable r = () -> {
            try {
                while (true) {
                    String val = cache.computeIfAbsent("key", (k) -> {
                        //Thread.sleep(10);
                        return "A";//String.valueOf(System.currentTimeMillis());
                    });
                    System.out.println(val);
                }
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        };
        for( int i = 0; i < 10; i++) {
            service.submit(r);
        }
        service.awaitTermination(1000, TimeUnit.DAYS);
    }

    public void testOwn() throws InterruptedException {
        org.elasticsearch.common.cache.Cache<String, String> cache = org.elasticsearch.common.cache.CacheBuilder.<String, String>builder().setExpireAfterAccess(TimeValue.parseTimeValue("5ms", "test"))
                .setExpireAfterWrite(TimeValue.parseTimeValue("5ms", "test")).build();
        ExecutorService service = Executors.newFixedThreadPool(10);
        Runnable r = () -> {
            try {
                while (true) {
                    String val = cache.computeIfAbsent("key", (k) -> {
                        //Thread.sleep(10);
                        return "A";//String.valueOf(System.currentTimeMillis());
                    });
                    System.out.println(val);
                }
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        };
        for( int i = 0; i < 10; i++) {
            service.submit(r);
        }
        service.awaitTermination(1000, TimeUnit.DAYS);
    }

    public void test2() {
        ReleasableLock lock = new ReleasableLock(new ReentrantLock());
        try(ReleasableLock lock2 = lock.acquire()) {
            try(ReleasableLock lock3 = lock.acquire()) {
                System.out.println(lock3.isHeldByCurrentThread());
            }
            System.out.println(lock2.isHeldByCurrentThread());
        }
        System.out.println(lock.isHeldByCurrentThread());
    }
}
