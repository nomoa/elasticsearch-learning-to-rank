package com.o19s.es.ltr.utils;

import org.elasticsearch.action.ActionListener;

import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

public interface AsyncCacheEntry<V,C> extends BiConsumer<C,ActionListener<V>> {
    boolean isAvailable();
    V getValue() throws ExecutionException;
    long weight();
}
