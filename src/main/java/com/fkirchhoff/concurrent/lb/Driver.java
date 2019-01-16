package com.fkirchhoff.concurrent.lb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public class Driver implements Runnable {
    private final long expected;
    long disableCount = 0;
    Random random = new Random(System.currentTimeMillis());
    List<String> nodes;
    Map<String, Boolean> nodesState;
    LoadBalancer<String> lb;
    Thread[] clients;
    Thread chaosMonkey;
    ConcurrentHashMap<String, LongAdder> freqs = new ConcurrentHashMap<>();
    private int iter;

    public Driver(int size, int nThreads, int iter) {
        expected = iter;
        nodes = new ArrayList<>(size);
        nodesState = new HashMap<>(size);
        this.iter = iter / nThreads;
        for (int c = 0; c < size; c++) {
            String chr = Character.toString((char) (c + 65));
            nodes.add(chr);
            nodesState.put(chr, Boolean.TRUE);
        }
        //lb = new RWLockLoadBalancer<>(nodesState.keySet(), 1000);
        lb = new ConcurrentLoadBalancer<>(nodesState.keySet(), 1000);
        clients = new Thread[nThreads];
        for (int i = 0; i < nThreads; i++) {
            clients[i] = new Thread(this::consume, "client-" + i);
        }
        chaosMonkey = new Thread(this::chaos, "chaos");
    }

    public static void main(String[] args) throws InterruptedException {
        Driver driver = new Driver(4, 100, 100_000_000);
        Thread runner = new Thread(driver);
        runner.start();
        runner.join();
        System.out.println("done");
    }

    private void chaos() {
        try {
            while (true) {
                boolean state = random.nextBoolean();
                // Fail only 1/2 of nodes
                String node = nodes.get(random.nextInt(nodes.size() / 2));
                nodesState.put(node, state);
                lb.retryDisabledNodes(nodesState::get);
                Thread.sleep(500);
            }
        } catch (InterruptedException e) {
        }
    }

    private void consume() {
        int i = iter;
        while (i-- > 0) {
            int tries = nodesState.size();
            String next;
            boolean isNodeUp;
            do {
                next = lb.next();
                isNodeUp = nodesState.get(next);
                if (!isNodeUp) {
                    lb.disableNode(next);
                }
                tries--;
            } while (!isNodeUp && tries > 0);
            if (isNodeUp) {
                freqs.computeIfAbsent(next, k -> new LongAdder()).increment();
            } else {
                disableCount++;
            }
        }
    }

    public void run() {
        try {
            long t0 = System.currentTimeMillis();
            for (int i = 0; i < clients.length; i++) {
                clients[i].start();
            }
            chaosMonkey.start();
            for (int i = 0; i < clients.length; i++) {
                clients[i].join();
            }
            chaosMonkey.interrupt();
            long dt = System.currentTimeMillis() - t0;
            System.out.println("Over in " + dt + " ms");
            freqs.entrySet().forEach(e -> System.out.printf("%s->%d\n", e.getKey(), e.getValue().longValue()));
            Long total = freqs.values().stream().map(LongAdder::longValue).reduce(0L, (x, l) -> x + l);
            System.out.println("Total failures: " + (expected - total) + " out of " + expected + " = " + String.format("%.10f", 100.0 * (expected - total) / expected) + " %");
            System.out.println("Disabled count=" + disableCount);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
