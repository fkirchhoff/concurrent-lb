package com.fkirchhoff.concurrent.lb;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RWLockLoadBalancer<T> implements LoadBalancer<T> {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final LinkedList<T> upNodes;
    private final ConcurrentHashMap<T, Long> downNodes;
    private final AtomicInteger counter = new AtomicInteger();
    private long delay;

    public RWLockLoadBalancer(Collection<T> nodes, long delay) {
        this.upNodes = new LinkedList<>(nodes);
        this.delay = delay;
        this.downNodes = new ConcurrentHashMap<>();
    }

    @Override
    public T next() {
        try {
            lock.readLock().lock();
            return upNodes.get(counter.incrementAndGet() % upNodes.size());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void disableNode(T node) {
        try {
            lock.writeLock().lock();
            upNodes.remove(node);
            long ts = System.currentTimeMillis() + delay;
            downNodes.put(node, ts);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void retryDisabledNodes(Function<T, Boolean> test) {
        try {
            lock.writeLock().lock();
            Long now = System.currentTimeMillis();
            List<T> candidates = downNodes.entrySet().stream()
                    .filter(e -> e.getValue() < now).filter(e -> test.apply(e.getKey()))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
            if (candidates.size() > 0) {
                upNodes.addAll(candidates);
                candidates.forEach(downNodes::remove);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}
