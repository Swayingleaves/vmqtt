package com.vmqtt.vmqttcore.service.router;

import java.util.List;

/**
 * 负载均衡接口
 */
public interface LoadBalancer<T> {
    /**
     * 从候选列表中选择一个目标
     */
    T select(List<T> candidates, String key);
}


