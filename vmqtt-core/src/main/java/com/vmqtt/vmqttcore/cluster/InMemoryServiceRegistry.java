package com.vmqtt.vmqttcore.cluster;

import com.google.protobuf.Timestamp;
import com.vmqtt.common.grpc.cluster.DiscoverServicesResponse;
import com.vmqtt.common.grpc.cluster.HealthStatus;
import com.vmqtt.common.grpc.cluster.ServiceInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * 轻量内存服务注册表（无外部中间件）
 */
@Slf4j
public class InMemoryServiceRegistry implements ServiceRegistry {

    // serviceName -> serviceId -> entry
    private final ConcurrentMap<String, ConcurrentMap<String, ServiceEntry>> registry = new ConcurrentHashMap<>();

    @Override
    public String register(ServiceInfo info, int ttlSeconds) {
        String serviceName = info.getServiceName();
        String serviceId = info.getServiceId().isEmpty() ? UUID.randomUUID().toString() : info.getServiceId();
        ServiceEntry entry = new ServiceEntry(serviceId, info, ttlSeconds, Instant.now().plusSeconds(ttlSeconds), HealthStatus.HEALTHY);
        registry.computeIfAbsent(serviceName, k -> new ConcurrentHashMap<>()).put(serviceId, entry);
        log.info("Service registered: name={}, id={}, addr={}:{}", serviceName, serviceId, info.getAddress(), info.getPort());
        return serviceId;
    }

    @Override
    public boolean unregister(String serviceId) {
        for (ConcurrentMap<String, ServiceEntry> map : registry.values()) {
            if (map.remove(serviceId) != null) {
                log.info("Service unregistered: id={}", serviceId);
                return true;
            }
        }
        return false;
    }

    @Override
    public List<ServiceInfo> discover(String serviceName) {
        cleanupExpired();
        Map<String, ServiceEntry> map = registry.getOrDefault(serviceName, new ConcurrentHashMap<>());
        return map.values().stream().map(ServiceEntry::toProto).collect(Collectors.toList());
    }

    @Override
    public boolean heartbeat(String serviceId, HealthStatus status) {
        for (ConcurrentMap<String, ServiceEntry> map : registry.values()) {
            ServiceEntry entry = map.get(serviceId);
            if (entry != null) {
                entry.setExpiresAt(Instant.now().plusSeconds(entry.getTtlSeconds()));
                entry.setHealthStatus(status);
                return true;
            }
        }
        return false;
    }

    @Override
    public DiscoverServicesResponse select(String serviceName) {
        List<ServiceInfo> services = discover(serviceName);
        ServiceInfo selected = services.isEmpty() ? null : services.get((int)(System.nanoTime() % services.size()));
        DiscoverServicesResponse.Builder builder = DiscoverServicesResponse.newBuilder()
            .setSuccess(true)
            .addAllServices(services);
        if (selected != null) {
            builder.setErrorMessage("");
        }
        return builder.build();
    }

    public void cleanupExpired() {
        Instant now = Instant.now();
        for (ConcurrentMap<String, ServiceEntry> map : registry.values()) {
            map.values().removeIf(entry -> entry.getExpiresAt().isBefore(now));
        }
    }

    @Data
    @AllArgsConstructor
    private static class ServiceEntry {
        private String serviceId;
        private ServiceInfo info;
        private int ttlSeconds;
        private Instant expiresAt;
        private HealthStatus healthStatus = HealthStatus.HEALTHY;

        public void setExpiresAt(Instant expiresAt) { this.expiresAt = expiresAt; }
        public int getTtlSeconds() { return ttlSeconds; }
        public Instant getExpiresAt() { return expiresAt; }

        public ServiceInfo toProto() {
            ServiceInfo.Builder b = ServiceInfo.newBuilder()
                .setServiceId(info.getServiceId())
                .setServiceName(info.getServiceName())
                .setServiceVersion(info.getServiceVersion())
                .setNodeId(info.getNodeId())
                .setAddress(info.getAddress())
                .setPort(info.getPort())
                .setHealthStatus(healthStatus);
            if (info.hasRegisteredAt()) {
                b.setRegisteredAt(info.getRegisteredAt());
            } else {
                b.setRegisteredAt(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build());
            }
            b.putAllMetadata(info.getMetadataMap());
            b.putAllTags(info.getTagsMap());
            return b.build();
        }
    }
}


