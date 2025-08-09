package com.vmqtt.vmqttcore.service.router;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 简单的轮询负载均衡实现
 */
public class RoundRobinLoadBalancer<T> implements LoadBalancer<T> {
    private final ConcurrentMap<String, AtomicInteger> keyToCounter = new ConcurrentHashMap<>();

    @Override
    public T select(List<T> candidates, String key) {
        if (candidates == null || candidates.isEmpty()) return null;
        AtomicInteger counter = keyToCounter.computeIfAbsent(key == null ? "__default__" : key, k -> new AtomicInteger(0));
        int idx = Math.floorMod(counter.getAndIncrement(), candidates.size());
        return candidates.get(idx);
    }
}


