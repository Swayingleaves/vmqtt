/**
 * 认证服务接口
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.service;

import com.vmqtt.common.model.ClientConnection;
import com.vmqtt.common.protocol.packet.connect.MqttConnectPacket;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * 认证服务接口
 * 负责MQTT客户端认证、授权和访问控制
 */
public interface AuthenticationService {

    /**
     * 验证客户端连接
     *
     * @param connectPacket 连接包
     * @param connection 连接信息
     * @return 认证结果
     */
    CompletableFuture<AuthenticationResult> authenticate(MqttConnectPacket connectPacket, ClientConnection connection);

    /**
     * 检查客户端发布权限
     *
     * @param clientId 客户端ID
     * @param topic 发布主题
     * @return 是否有发布权限
     */
    CompletableFuture<Boolean> checkPublishPermission(String clientId, String topic);

    /**
     * 检查客户端订阅权限
     *
     * @param clientId 客户端ID
     * @param topicFilter 订阅主题过滤器
     * @return 是否有订阅权限
     */
    CompletableFuture<Boolean> checkSubscribePermission(String clientId, String topicFilter);

    /**
     * 验证客户端ID是否有效
     *
     * @param clientId 客户端ID
     * @return 是否有效
     */
    boolean isValidClientId(String clientId);

    /**
     * 获取客户端权限信息
     *
     * @param clientId 客户端ID
     * @return 权限信息
     */
    ClientPermissions getClientPermissions(String clientId);

    /**
     * 创建认证凭据
     *
     * @param clientId 客户端ID
     * @param username 用户名
     * @param password 密码
     * @param attributes 额外属性
     * @return 认证凭据
     */
    CompletableFuture<AuthenticationCredentials> createCredentials(String clientId, 
                                                                  String username, 
                                                                  String password, 
                                                                  Map<String, Object> attributes);

    /**
     * 撤销认证凭据
     *
     * @param clientId 客户端ID
     * @return 操作结果
     */
    CompletableFuture<Boolean> revokeCredentials(String clientId);

    /**
     * 更新客户端权限
     *
     * @param clientId 客户端ID
     * @param permissions 新权限
     * @return 操作结果
     */
    CompletableFuture<Boolean> updatePermissions(String clientId, ClientPermissions permissions);

    /**
     * 检查客户端是否被禁用
     *
     * @param clientId 客户端ID
     * @return 是否被禁用
     */
    boolean isClientBlocked(String clientId);

    /**
     * 禁用客户端
     *
     * @param clientId 客户端ID
     * @param reason 禁用原因
     * @return 操作结果
     */
    CompletableFuture<Boolean> blockClient(String clientId, String reason);

    /**
     * 解除客户端禁用
     *
     * @param clientId 客户端ID
     * @return 操作结果
     */
    CompletableFuture<Boolean> unblockClient(String clientId);

    /**
     * 获取认证统计信息
     *
     * @return 认证统计信息
     */
    AuthenticationStats getAuthenticationStats();

    /**
     * 注册认证事件监听器
     *
     * @param listener 认证事件监听器
     */
    void addAuthenticationListener(AuthenticationEventListener listener);

    /**
     * 移除认证事件监听器
     *
     * @param listener 认证事件监听器
     */
    void removeAuthenticationListener(AuthenticationEventListener listener);

    /**
     * 认证结果
     */
    interface AuthenticationResult {
        /**
         * 认证是否成功
         *
         * @return 认证成功返回true
         */
        boolean isSuccess();

        /**
         * 获取失败原因
         *
         * @return 失败原因
         */
        String getFailureReason();

        /**
         * 获取认证返回码
         *
         * @return 返回码
         */
        AuthenticationCode getReturnCode();

        /**
         * 获取客户端权限
         *
         * @return 客户端权限
         */
        ClientPermissions getPermissions();

        /**
         * 获取认证信息
         *
         * @return 认证信息
         */
        AuthenticationCredentials getCredentials();

        /**
         * 获取会话过期时间（秒）
         *
         * @return 会话过期时间
         */
        int getSessionExpiryInterval();
    }

    /**
     * 认证返回码
     */
    enum AuthenticationCode {
        /**
         * 连接接受
         */
        CONNECTION_ACCEPTED,

        /**
         * 协议版本不支持
         */
        UNACCEPTABLE_PROTOCOL_VERSION,

        /**
         * 客户端ID无效
         */
        IDENTIFIER_REJECTED,

        /**
         * 服务器不可用
         */
        SERVER_UNAVAILABLE,

        /**
         * 用户名密码错误
         */
        BAD_USERNAME_PASSWORD,

        /**
         * 未授权
         */
        NOT_AUTHORIZED,

        /**
         * 客户端被禁用
         */
        CLIENT_BLOCKED,

        /**
         * 认证失败
         */
        AUTHENTICATION_FAILED
    }

    /**
     * 客户端权限
     */
    interface ClientPermissions {
        /**
         * 是否允许连接
         *
         * @return 允许连接返回true
         */
        boolean canConnect();

        /**
         * 是否允许发布到指定主题
         *
         * @param topic 主题
         * @return 允许发布返回true
         */
        boolean canPublishTo(String topic);

        /**
         * 是否允许订阅指定主题过滤器
         *
         * @param topicFilter 主题过滤器
         * @return 允许订阅返回true
         */
        boolean canSubscribeTo(String topicFilter);

        /**
         * 获取最大QoS级别
         *
         * @return 最大QoS级别
         */
        int getMaxQos();

        /**
         * 获取最大消息大小
         *
         * @return 最大消息大小（字节）
         */
        int getMaxMessageSize();

        /**
         * 获取最大排队消息数
         *
         * @return 最大排队消息数
         */
        int getMaxQueueSize();

        /**
         * 获取速率限制（消息/秒）
         *
         * @return 速率限制
         */
        int getRateLimit();

        /**
         * 获取会话过期间隔（秒）
         *
         * @return 会话过期间隔
         */
        int getSessionExpiryInterval();

        /**
         * 获取权限属性
         *
         * @return 权限属性
         */
        Map<String, Object> getAttributes();
    }

    /**
     * 认证凭据
     */
    interface AuthenticationCredentials {
        /**
         * 获取客户端ID
         *
         * @return 客户端ID
         */
        String getClientId();

        /**
         * 获取用户名
         *
         * @return 用户名
         */
        String getUsername();

        /**
         * 获取密码哈希
         *
         * @return 密码哈希
         */
        String getPasswordHash();

        /**
         * 获取凭据创建时间
         *
         * @return 创建时间
         */
        long getCreatedAt();

        /**
         * 获取凭据过期时间
         *
         * @return 过期时间
         */
        long getExpiresAt();

        /**
         * 检查凭据是否过期
         *
         * @return 过期返回true
         */
        boolean isExpired();

        /**
         * 获取凭据属性
         *
         * @return 凭据属性
         */
        Map<String, Object> getAttributes();
    }

    /**
     * 认证统计信息
     */
    interface AuthenticationStats {
        /**
         * 获取总认证次数
         *
         * @return 总认证次数
         */
        long getTotalAuthentications();

        /**
         * 获取成功认证次数
         *
         * @return 成功认证次数
         */
        long getSuccessfulAuthentications();

        /**
         * 获取失败认证次数
         *
         * @return 失败认证次数
         */
        long getFailedAuthentications();

        /**
         * 获取被禁用客户端数
         *
         * @return 被禁用客户端数
         */
        long getBlockedClients();

        /**
         * 获取活跃凭据数
         *
         * @return 活跃凭据数
         */
        long getActiveCredentials();

        /**
         * 获取过期凭据数
         *
         * @return 过期凭据数
         */
        long getExpiredCredentials();

        /**
         * 获取认证成功率
         *
         * @return 认证成功率
         */
        double getAuthenticationSuccessRate();

        /**
         * 获取平均认证时间（毫秒）
         *
         * @return 平均认证时间
         */
        double getAverageAuthenticationTime();
    }

    /**
     * 认证事件监听器
     */
    interface AuthenticationEventListener {
        /**
         * 认证成功事件
         *
         * @param clientId 客户端ID
         * @param result 认证结果
         */
        void onAuthenticationSuccess(String clientId, AuthenticationResult result);

        /**
         * 认证失败事件
         *
         * @param clientId 客户端ID
         * @param result 认证结果
         */
        void onAuthenticationFailure(String clientId, AuthenticationResult result);

        /**
         * 权限检查失败事件
         *
         * @param clientId 客户端ID
         * @param action 操作类型（publish/subscribe）
         * @param resource 资源（主题）
         */
        void onPermissionDenied(String clientId, String action, String resource);

        /**
         * 客户端被禁用事件
         *
         * @param clientId 客户端ID
         * @param reason 禁用原因
         */
        void onClientBlocked(String clientId, String reason);

        /**
         * 客户端解禁事件
         *
         * @param clientId 客户端ID
         */
        void onClientUnblocked(String clientId);

        /**
         * 凭据过期事件
         *
         * @param clientId 客户端ID
         * @param credentials 过期凭据
         */
        void onCredentialsExpired(String clientId, AuthenticationCredentials credentials);
    }
}