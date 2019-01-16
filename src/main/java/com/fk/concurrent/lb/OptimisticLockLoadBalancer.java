package com.fk.concurrent.lb;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

public class OptimisticLockLoadBalancer<T> implements LoadBalancer<T> {
    private final CopyOnWriteArrayList<T> upNodes;
    private final ConcurrentHashMap<T, Long> downNodes;
    private final AtomicInteger counter = new AtomicInteger();
    private long delay;

    public OptimisticLockLoadBalancer(Collection<T> nodes, long delay) {
        this.upNodes = new CopyOnWriteArrayList<>(nodes);
        this.delay = delay;
        this.downNodes = new ConcurrentHashMap<>();
    }

    public T next() {
        T next = null;
        int tries = upNodes.size();
        while (next == null || tries > 0) {
            try {
                return upNodes.get(counter.incrementAndGet() % upNodes.size());
            } catch (IndexOutOfBoundsException ex) {
            } finally {
                tries--;
            }
        }
        return next;
    }

    public void disableNode(T node) {
        upNodes.remove(node);
        long ts = System.currentTimeMillis() + delay;
        downNodes.put(node, ts);
    }

    public void retryDisabledNodes(Function<T, Boolean> test) {
        Long now = System.currentTimeMillis();
        List<T> candidates = downNodes.entrySet().stream()
                .filter(e -> e.getValue() < now).filter(e -> test.apply(e.getKey()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        if (candidates.size() > 0) {
            upNodes.addAll(candidates);
            candidates.forEach(downNodes::remove);
        }
    }
}
