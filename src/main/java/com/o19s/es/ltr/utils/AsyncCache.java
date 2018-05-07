package com.o19s.es.ltr.utils;

import org.apache.lucene.util.mutable.MutableValueLong;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ReleasableLock;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;
import java.util.function.Function;

public class AsyncCache<K,V,C> {
    private final ExecutorService executor;
    private final AsyncLoadFunction<C,K,V> asyncLoadFunction;
    private final StampedLock lock = new StampedLock();
    private final ConcurrentHashMap<K,WeakReference<LoadingEntry>> loadingEntries = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<K,LoadedEntry<K, V, C>> loadedEntries = new ConcurrentHashMap<>();
    private LoadedEntry<K, V, C> head;
    private LoadedEntry<K, V, C> tail;

    private final static <V> Function<V, Long> defaultWeightFunction() {
        return (v) -> 1L;
    }
    private final static <V> Consumer<V> defaultListener() {
        return (v) -> {};
    }
    private Function<V, Long> weightFunction = defaultWeightFunction();
    private Consumer<AsyncCacheEntry<V,C>> onLoadListener = defaultListener();
    private Consumer<AsyncCacheEntry<V,C>> onRemoveListener = defaultListener();

    private boolean serializeDependentReads;
    private int maxDependentReads;
    private long expireAfterAccessNanos = -1;
    private boolean entriesExpireAfterAccess;
    private long expireAfterWriteNanos = -1;
    private boolean entriesExpireAfterWrite;
    private long maxWeight;
    private LongAdder currentWeight = new LongAdder();

    private final LongAdder hits = new LongAdder();
    private final LongAdder misses = new LongAdder();

    public AsyncCache(ExecutorService executor, AsyncLoadFunction<C,K,V> asyncLoadFunction, int maxDependentReads, boolean serializeDependentReads) {
        this.executor = executor;
        this.asyncLoadFunction = asyncLoadFunction;
        this.maxDependentReads = maxDependentReads;
        this.serializeDependentReads = serializeDependentReads;
    }

    private long now() {
        return entriesExpireAfterAccess || entriesExpireAfterWrite ? System.nanoTime() : 0;
    }

    private boolean isExpired(LoadedEntry<K, V, C> entry, long now) {
        return (entriesExpireAfterWrite && (expireAfterWriteNanos + entry.receivedAt > now)) ||
                (entriesExpireAfterAccess && (expireAfterAccessNanos + entry.lastAccess) > now);
    }

    public AsyncCacheEntry<V,C> asyncGet(K key) {
        long optimistic = lock.tryOptimisticRead();
        long now = now();
        if (optimistic > 0) {
            LoadedEntry<K, V, C> value = loadedEntries.get(key);
            if (value != null && head == value && !isExpired(value, now) && lock.validate(optimistic)) {
                hit(value, now);
                return value;
            }
        }
        MutableValueLong rLock = new MutableValueLong();
        rLock.value = lock.readLock();
        try {
            // Remove stale entries
            loadingEntries.entrySet().removeIf(entry -> entry.getValue().get() == null);
            final Suppliers.MutableSupplier<Object> keepRef = new Suppliers.MutableSupplier<>();
            WeakReference<LoadingEntry> weakLoader = loadingEntries.compute(key, (k, v) -> {
                if (loadedEntries.containsKey(k)) {
                    return null;
                }
                if (v == null) {
                    LoadingEntry en = new LoadingEntry(key);
                    return new WeakReference<>(en);
                } else {
                    LoadingEntry entry = v.get();
                    if (entry == null) {
                        return new WeakReference<>(new LoadingEntry(key));
                    }
                    // Just to be sure that this value is not reclaimed
                    // before we use it.
                    keepRef.set(entry);
                    return v;
                }
            });
            if (weakLoader != null) {
                LoadingEntry en = weakLoader.get();
                assert en != null;
                return en;
            }
            LoadedEntry<K, V,C> loaded = loadedEntries.get(key);
            assert loaded != null;
            moveToHead(loaded, rLock);
            return loaded;
        } finally {
            lock.unlockRead(rLock.value);
        }
    }

    private void hit(LoadedEntry<K,V,C> value, long now) {
        hit(value, now, 1);
    }

    private void hit(LoadedEntry<K,V,C> value, long now, long nbHits) {
        hits.add(nbHits);
        if (entriesExpireAfterAccess) {
            value.lastAccess = now;
        }
    }

    private void moveToHead(LoadedEntry<K, V, C> loadedEntry, MutableValueLong readLock) {
        if (head == loadedEntry) {
            return;
        }
        assert lock.validate(readLock.value);
        long writeLock = lock.tryConvertToWriteLock(readLock.value);
        if (writeLock == 0) {
            lock.unlockRead(readLock.value);
            writeLock = lock.writeLock();
        }
        try {
            if (head == null) {
                assert tail == null;
                head = tail = loadedEntry;
            } else if (head != loadedEntry) {
                LoadedEntry<K, V, C> prev = loadedEntry.prev;
                LoadedEntry<K, V, C> next = loadedEntry.next;
                assert prev != null;
                prev.next = next;
                if (next == null) {
                    tail = prev;
                }
                loadedEntry.prev = null;
                loadedEntry.next = head;
                head.next = loadedEntry;
                head = loadedEntry;
            }
        } finally {
            readLock.value = lock.tryConvertToReadLock(writeLock);
        }
    }

    public boolean invalidate(K key) {
        long rwLock = lock.writeLock();
        try {
            LoadedEntry<K, V, C> entry = loadedEntries.remove(key);
            if (entry == null) {
                return false;
            }
            if (entry == head) {
                head = entry.next;
                assert entry.prev == null;
                if (head == null) {
                    tail = null;
                    assert loadedEntries.isEmpty();
                } else {
                    head.prev = null;
                }
            } else if (entry == tail) {
                assert entry.prev != null;
                tail = entry.prev;
                tail.next = null;
            } else {
                entry.prev.next = entry.next;
                entry.next.prev = entry.prev;
            }
            onRemoveListener.accept(entry);
            return true;
        } finally {
            lock.unlockWrite(rwLock);
        }
    }

    private LoadedEntry<K, V, C> promote(LoadingEntry promoted, long nbHits, long now) {
        long wLock = lock.writeLock();
        LoadedEntry<K, V, C> entry;
        try {
            entry = new LoadedEntry<>(promoted.key, promoted.value.get(), promoted.weight, promoted.receivedAt);
            Object prev = loadedEntries.put(entry.key, entry);
            assert prev == null;
            prev = loadingEntries.remove(promoted.key);
            assert prev != null && ((WeakReference) prev).get() != null;
            if (head == null) {
                assert tail == null;
                head = tail = entry;
            } else {
                entry.next = head;
                head.prev = entry;
                head = entry;
            }
            hit(entry, now, nbHits);
            onLoadListener.accept(entry);
            return entry;
        } finally {
            lock.unlock(wLock);
        }
    }

    private void invalidateLoadingEntry(LoadingEntry entry) {
        long wLock = lock.writeLock();
        try {
            loadingEntries.remove(entry.key);
        } finally {
            lock.unlock(wLock);
        }
    }

    private ExecutorService executor() {
        return executor;
    }

    private class LoadingEntry implements AsyncCacheEntry<V, C> {
        private final K key;
        private final AtomicReference<V> value = new AtomicReference<>();
        private final AtomicReference<Exception> exception = new AtomicReference<>();
        private final long createdAt = System.currentTimeMillis();
        private long firedAt;
        private long receivedAt;
        private long weight;
        private AtomicBoolean fired = new AtomicBoolean();
        private ReleasableLock lock = new ReleasableLock(new ReentrantLock());
        private AtomicBoolean received = new AtomicBoolean();
        private Collection<ActionListener<V>> waitingListeners = new ArrayList<>();

        private LoadingEntry(K key) {
            this.key = key;
        }

        @Override
        public boolean isAvailable() {
            return received.get();
        }

        @Override
        public V getValue() throws ExecutionException {
            if (!received.get()) {
                throw new IllegalStateException();
            }
            if (exception.get() != null) {
                throw new ExecutionException(exception.get());
            }
            return value.get();
        }

        @Override
        public long weight() {
            return weight;
        }

        @Override
        public void accept(C c, ActionListener<V> listener) {
            if (received.get()) {
                if (exception.get() != null) {
                    listener.onFailure(exception.get());
                }
                // Directly increment hits, ideally we should
                // retrieve the respective LoadedEntry but this seems
                // too risky to do so, it should rarely happen since
                // we won the race between the onResponse reception
                // and the promote call. Having wrong lastAccessTime
                // is preferable to excessive locking.
                hits.increment();
                listener.onResponse(value.get());
            } else {

                boolean added = false;
                try (ReleasableLock unused = lock.acquire()) {
                    if (!received.get()) {
                        if (fired.get() && waitingListeners.size() >= maxDependentReads) {
                            throw new EsRejectedExecutionException("Rejected execution AsyncCache max dependent reads reached " + maxDependentReads, executor.isShutdown());
                        }
                        waitingListeners.add(listener);
                        added = true;
                    }
                }
                if (!added) {
                    if (exception.get() != null) {
                        listener.onFailure(exception.get());
                    }
                    listener.onResponse(value.get());
                    return;
                }
                if (fired.compareAndSet(false, true)) {
                    misses.increment();
                    firedAt = System.nanoTime();
                    asyncLoadFunction.accept(c, key, new ActionListener<V>() {
                        @Override
                        public void onResponse(V v) {
                            long now = now();
                            Collection<ActionListener<V>> listeners;
                            value.set(v);
                            weight = weightFunction.apply(v);
                            try (ReleasableLock unused = lock.acquire()) {
                                received.set(true);
                                receivedAt = now;
                                assert waitingListeners != null;
                                listeners = waitingListeners;
                                waitingListeners = null;
                            }
                            assert !listeners.isEmpty();
                            LoadedEntry<K, V, C> entry = promote(LoadingEntry.this, now, listeners.size()-1);
                            receivedAt = now;
                            if (serializeDependentReads) {
                                ActionListener.onResponse(listeners, v);
                            } else {
                                listeners.forEach((l) -> {
                                    try {
                                        executor.submit(() -> l.onResponse(v));
                                    } catch (Exception e) {
                                        l.onFailure(e);
                                    }
                                } );
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            this.onFailure(e, false);
                        }

                        public void onFailure(Exception e, boolean alreadyInvalidated) {
                            exception.set(e);
                            invalidateLoadingEntry(LoadingEntry.this);
                            Collection<ActionListener<V>> listeners;
                            try (ReleasableLock unused = lock.acquire()) {
                                received.set(true);
                                assert waitingListeners != null;
                                listeners = waitingListeners;
                                waitingListeners = null;
                            }
                            receivedAt = System.nanoTime();
                            ActionListener.onFailure(listeners, e);
                        }
                    });
                }
            }
        }
    }

    private static class LoadedEntry<K, V, C> implements AsyncCacheEntry<V, C> {
        private final K key;
        private final V value;
        private final long weight;
        private final long receivedAt;
        private volatile long lastAccess;
        private LoadedEntry<K, V, C> prev;
        private LoadedEntry<K, V, C> next;

        private LoadedEntry(K key, V value, long weight, long receivedAt) {
            this.key = key;
            this.value = value;
            this.weight = weight;
            this.receivedAt = receivedAt;
        }

        @Override
        public boolean isAvailable() {
            return true;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public long weight() {
            return weight;
        }

        @Override
        public void accept(C v, ActionListener<V> actionListener) {
            actionListener.onResponse(value);
        }
    }
}
