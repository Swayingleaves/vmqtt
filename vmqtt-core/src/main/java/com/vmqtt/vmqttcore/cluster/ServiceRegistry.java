package com.vmqtt.vmqttcore.cluster;

import com.vmqtt.common.grpc.cluster.DiscoverServicesResponse;
import com.vmqtt.common.grpc.cluster.HealthStatus;
import com.vmqtt.common.grpc.cluster.ServiceInfo;

import java.util.List;

/**
 * 轻量级服务注册/发现接口（无外部中间件）
 */
public interface ServiceRegistry {
    String register(ServiceInfo info, int ttlSeconds);
    boolean unregister(String serviceId);
    List<ServiceInfo> discover(String serviceName);
    boolean heartbeat(String serviceId, HealthStatus status);
    DiscoverServicesResponse select(String serviceName);
}


