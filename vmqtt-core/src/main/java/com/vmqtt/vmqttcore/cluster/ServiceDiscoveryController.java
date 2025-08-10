package com.vmqtt.vmqttcore.cluster;

import com.vmqtt.common.grpc.cluster.HealthStatus;
import com.vmqtt.common.grpc.cluster.ServiceInfo;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/api/v1/discovery")
@RequiredArgsConstructor
public class ServiceDiscoveryController {

    private final ServiceRegistry serviceRegistry;

    @GetMapping("/services")
    public ResponseEntity<List<Map<String, Object>>> list(@RequestParam("name") String serviceName) {
        List<ServiceInfo> services = serviceRegistry.discover(serviceName);
        return ResponseEntity.ok(services.stream().map(this::toMap).collect(Collectors.toList()));
    }

    @GetMapping("/select")
    public ResponseEntity<Map<String, Object>> select(@RequestParam("name") String serviceName) {
        // 使用内置选择策略：返回排序后第一项作为选中节点
        List<ServiceInfo> services = serviceRegistry.discover(serviceName);
        ServiceInfo selected = WeightedSelector.selectBest(services);
        return selected == null ? ResponseEntity.noContent().build() : ResponseEntity.ok(toMap(selected));
    }

    @PostMapping("/register")
    public ResponseEntity<Map<String, Object>> register(@RequestBody RegisterRequest request) {
        ServiceInfo info = request.toServiceInfo();
        String id = serviceRegistry.register(info, request.getTtlSeconds() == null ? 30 : request.getTtlSeconds());
        return ResponseEntity.ok(Collections.singletonMap("serviceId", id));
    }

    @PostMapping("/unregister")
    public ResponseEntity<Map<String, Object>> unregister(@RequestBody Map<String, String> body) {
        boolean ok = serviceRegistry.unregister(body.get("serviceId"));
        return ResponseEntity.ok(Collections.singletonMap("success", ok));
    }

    @PostMapping("/heartbeat")
    public ResponseEntity<Map<String, Object>> heartbeat(@RequestBody Map<String, String> body) {
        String serviceId = body.get("serviceId");
        HealthStatus status = body.get("status") == null ? HealthStatus.HEALTHY : HealthStatus.valueOf(body.get("status"));
        boolean ok = serviceRegistry.heartbeat(serviceId, status);
        return ResponseEntity.ok(Collections.singletonMap("success", ok));
    }

    @PostMapping("/gossip")
    public ResponseEntity<Map<String, Object>> gossip(@RequestBody GossipPayload payload) {
        int ttl = payload.getTtlSeconds() == null ? 30 : payload.getTtlSeconds();
        for (ServiceInfo info : payload.toServiceInfos()) {
            serviceRegistry.register(info, ttl);
        }
        return ResponseEntity.ok(Collections.singletonMap("merged", payload.getServices().size()));
    }

    @GetMapping("/metrics")
    public ResponseEntity<Map<String, Object>> metrics(@RequestParam(value = "name", required = false) String serviceName) {
        List<ServiceInfo> all = serviceName == null ? getAll() : serviceRegistry.discover(serviceName);
        Map<String, Long> byName = all.stream().collect(Collectors.groupingBy(ServiceInfo::getServiceName, Collectors.counting()));
        long healthy = all.stream().filter(s -> s.getHealthStatus() == HealthStatus.HEALTHY).count();
        long unhealthy = all.size() - healthy;
        Map<String, Object> res = new HashMap<>();
        res.put("total", all.size());
        res.put("healthy", healthy);
        res.put("unhealthy", unhealthy);
        res.put("byServiceName", byName);
        return ResponseEntity.ok(res);
    }

    private List<ServiceInfo> getAll() {
        // 简易聚合：无全量接口则从名称集合推导，这里假设常见服务名列表可以通过配置传入。为简化返回空集合。
        return Collections.emptyList();
    }

    private Map<String, Object> toMap(ServiceInfo s) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("serviceId", s.getServiceId());
        m.put("serviceName", s.getServiceName());
        m.put("serviceVersion", s.getServiceVersion());
        m.put("nodeId", s.getNodeId());
        m.put("address", s.getAddress());
        m.put("port", s.getPort());
        m.put("healthStatus", s.getHealthStatus().name());
        m.put("metadata", s.getMetadataMap());
        m.put("tags", s.getTagsMap());
        return m;
    }

    @Data
    public static class RegisterRequest {
        private String serviceId;
        private String serviceName;
        private String serviceVersion;
        private String nodeId;
        private String address;
        private Integer port;
        private Map<String, String> metadata = new HashMap<>();
        private Map<String, String> tags = new HashMap<>();
        private Integer ttlSeconds;

        public ServiceInfo toServiceInfo() {
            ServiceInfo.Builder b = ServiceInfo.newBuilder()
                .setServiceId(Optional.ofNullable(serviceId).orElse(""))
                .setServiceName(serviceName)
                .setServiceVersion(Optional.ofNullable(serviceVersion).orElse(""))
                .setNodeId(Optional.ofNullable(nodeId).orElse(""))
                .setAddress(address == null ? "127.0.0.1" : address)
                .setPort(port == null ? 0 : port)
                .setHealthStatus(HealthStatus.HEALTHY);
            if (metadata != null) b.putAllMetadata(metadata);
            if (tags != null) b.putAllTags(tags);
            return b.build();
        }
    }

    @Data
    public static class GossipPayload {
        private List<RegisterRequest> services = new ArrayList<>();
        private Integer ttlSeconds;

        public List<ServiceInfo> toServiceInfos() {
            return services.stream().map(RegisterRequest::toServiceInfo).collect(Collectors.toList());
        }
    }

    /**
     * 节点选择策略：按健康度过滤 + 权重随机（metadata.weight），若相同可使用响应时间加权（metadata.response_time）。
     */
    static class WeightedSelector {
        static ServiceInfo selectBest(List<ServiceInfo> services) {
            if (services == null || services.isEmpty()) return null;
            List<ServiceInfo> healthy = services.stream().filter(s -> s.getHealthStatus() == HealthStatus.HEALTHY).collect(Collectors.toList());
            List<ServiceInfo> pool = healthy.isEmpty() ? services : healthy;
            // 读取权重，默认100
            List<Weighted> items = new ArrayList<>();
            for (ServiceInfo s : pool) {
                int w = 100;
                if (s.getMetadataMap().containsKey("weight")) {
                    try { w = Math.max(1, Integer.parseInt(s.getMetadataMap().get("weight"))); } catch (Exception ignored) {}
                }
                // 如果有响应时间，按 1/rt 做微调
                double adj = 1.0;
                if (s.getMetadataMap().containsKey("response_time")) {
                    try { double rt = Double.parseDouble(s.getMetadataMap().get("response_time")); adj = rt > 0 ? (1000.0 / rt) : 1.0; } catch (Exception ignored) {}
                }
                items.add(new Weighted(s, Math.max(1.0, w * adj)));
            }
            double sum = items.stream().mapToDouble(Weighted::weight).sum();
            double r = Math.random() * sum;
            double acc = 0;
            for (Weighted it : items) {
                acc += it.weight();
                if (r <= acc) return it.info();
            }
            return items.get(items.size() - 1).info();
        }

        record Weighted(ServiceInfo info, double weight) {}
    }
}


