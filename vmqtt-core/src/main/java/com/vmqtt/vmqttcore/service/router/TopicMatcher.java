package com.vmqtt.vmqttcore.service.router;

/**
 * MQTT 主题匹配工具
 * 支持 + 和 # 通配符
 */
public final class TopicMatcher {
    private TopicMatcher() {}

    public static boolean matches(String topic, String filter) {
        if (filter == null || topic == null) return false;
        if ("#".equals(filter)) return true;
        String[] t = topic.split("/", -1);
        String[] f = filter.split("/", -1);
        return matchLevels(t, f, 0, 0);
    }

    private static boolean matchLevels(String[] topic, String[] filter, int ti, int fi) {
        if (fi == filter.length) return ti == topic.length;
        String f = filter[fi];
        if ("#".equals(f)) return fi == filter.length - 1; // # must be last
        if (ti == topic.length) return false;
        if ("+".equals(f) || f.equals(topic[ti])) {
            return matchLevels(topic, filter, ti + 1, fi + 1);
        }
        return false;
    }
}


