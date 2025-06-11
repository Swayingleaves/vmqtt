#!/usr/bin/env python3
"""
跨FE节点消息分发测试脚本

测试场景：
1. FE1和FE2都启动
2. BE1只注册到FE1
3. Client1连接FE1，Client2连接FE2，都订阅topic1
4. Client1发布消息到topic1，验证Client2能否收到

@author zhenglin
@mail zhenglin.cn.cq@gmail.com
"""

import paho.mqtt.client as mqtt
import json
import time
import threading
import requests
import sys
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 测试配置
FE1_HOST = "localhost"
FE1_PORT = 1883
FE1_HTTP_PORT = 8081

FE2_HOST = "localhost"
FE2_PORT = 1884
FE2_HTTP_PORT = 8082

BE1_HOST = "localhost"
BE1_PORT = 9091

TEST_TOPIC = "topic1"
TEST_MESSAGE = "Hello from Client1 to Client2 across FE nodes!"

# 全局变量
client2_received_message = None
client2_message_event = threading.Event()

def on_connect_client1(client, userdata, flags, rc):
    """Client1连接回调"""
    if rc == 0:
        logger.info("Client1连接FE1成功")
    else:
        logger.error(f"Client1连接FE1失败，返回码: {rc}")

def on_connect_client2(client, userdata, flags, rc):
    """Client2连接回调"""
    if rc == 0:
        logger.info("Client2连接FE2成功")
        # 连接成功后订阅topic
        client.subscribe(TEST_TOPIC, qos=1)
        logger.info(f"Client2订阅topic: {TEST_TOPIC}")
    else:
        logger.error(f"Client2连接FE2失败，返回码: {rc}")

def on_message_client2(client, userdata, msg):
    """Client2接收消息回调"""
    global client2_received_message
    client2_received_message = msg.payload.decode('utf-8')
    logger.info(f"Client2收到消息: topic={msg.topic}, payload={client2_received_message}")
    client2_message_event.set()

def on_subscribe_client2(client, userdata, mid, granted_qos):
    """Client2订阅成功回调"""
    logger.info(f"Client2订阅成功，QoS: {granted_qos}")

def register_be_to_fe():
    """注册BE1到FE1"""
    try:
        url = f"http://{FE1_HOST}:{FE1_HTTP_PORT}/api/be/nodes"
        data = {
            "nodeId": "be1",
            "host": BE1_HOST,
            "port": BE1_PORT,
            "grpcPort": BE1_PORT + 1000
        }
        
        logger.info(f"注册BE1到FE1: {url}")
        response = requests.post(url, json=data, timeout=10)
        
        if response.status_code == 200:
            logger.info("BE1注册到FE1成功")
            return True
        else:
            logger.error(f"BE1注册到FE1失败: status={response.status_code}, response={response.text}")
            return False
            
    except Exception as e:
        logger.error(f"注册BE1到FE1异常: {e}")
        return False

def test_cross_fe_message_distribution():
    """测试跨FE节点消息分发"""
    logger.info("开始测试跨FE节点消息分发...")
    
    # 1. 注册BE1到FE1
    logger.info("步骤1: 注册BE1到FE1")
    if not register_be_to_fe():
        logger.error("注册BE1到FE1失败，测试终止")
        return False
    
    time.sleep(2)  # 等待注册生效
    
    # 2. 创建Client1连接FE1
    logger.info("步骤2: 创建Client1连接FE1")
    client1 = mqtt.Client(client_id="client1", clean_session=True)
    client1.on_connect = on_connect_client1
    
    try:
        client1.connect(FE1_HOST, FE1_PORT, 60)
        client1.loop_start()
        time.sleep(2)  # 等待连接建立
    except Exception as e:
        logger.error(f"Client1连接FE1失败: {e}")
        return False
    
    # 3. 创建Client2连接FE2
    logger.info("步骤3: 创建Client2连接FE2")
    client2 = mqtt.Client(client_id="client2", clean_session=True)
    client2.on_connect = on_connect_client2
    client2.on_message = on_message_client2
    client2.on_subscribe = on_subscribe_client2
    
    try:
        client2.connect(FE2_HOST, FE2_PORT, 60)
        client2.loop_start()
        time.sleep(3)  # 等待连接和订阅完成
    except Exception as e:
        logger.error(f"Client2连接FE2失败: {e}")
        client1.disconnect()
        return False
    
    # 4. Client1也订阅topic（验证发布者能收到自己的消息）
    logger.info("步骤4: Client1订阅topic")
    client1.subscribe(TEST_TOPIC, qos=1)
    time.sleep(2)
    
    # 5. Client1发布消息
    logger.info("步骤5: Client1发布消息")
    try:
        result = client1.publish(TEST_TOPIC, TEST_MESSAGE, qos=0)
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            logger.info(f"Client1发布消息成功: {TEST_MESSAGE}")
        else:
            logger.error(f"Client1发布消息失败: {result.rc}")
            client1.disconnect()
            client2.disconnect()
            return False
    except Exception as e:
        logger.error(f"Client1发布消息异常: {e}")
        client1.disconnect()
        client2.disconnect()
        return False
    
    # 6. 等待Client2接收消息
    logger.info("步骤6: 等待Client2接收消息...")
    message_received = client2_message_event.wait(timeout=10)
    print(f"message_received: {message_received}")
    
    # 7. 验证结果
    success = False
    if message_received and client2_received_message == TEST_MESSAGE:
        logger.info("✅ 测试成功！Client2成功接收到跨FE节点的消息")
        success = True
    else:
        logger.error("❌ 测试失败！Client2未接收到消息或消息内容不匹配")
        logger.error(f"期望消息: {TEST_MESSAGE}")
        logger.error(f"实际接收: {client2_received_message}")
    
    # 8. 清理资源
    logger.info("步骤8: 清理资源")
    try:
        client1.disconnect()
        client2.disconnect()
        client1.loop_stop()
        client2.loop_stop()
    except Exception as e:
        logger.warning(f"清理资源时出现异常: {e}")
    
    return success

def main():
    """主函数"""
    logger.info("=" * 60)
    logger.info("跨FE节点消息分发测试")
    logger.info("=" * 60)
    
    # 检查测试前提条件
    logger.info("检查测试前提条件...")
    logger.info(f"FE1: {FE1_HOST}:{FE1_PORT} (HTTP: {FE1_HTTP_PORT})")
    logger.info(f"FE2: {FE2_HOST}:{FE2_PORT} (HTTP: {FE2_HTTP_PORT})")
    logger.info(f"BE1: {BE1_HOST}:{BE1_PORT}")
    
    # 执行测试
    success = test_cross_fe_message_distribution()
    
    # 输出测试结果
    logger.info("=" * 60)
    if success:
        logger.info("🎉 跨FE节点消息分发测试通过！")
        sys.exit(0)
    else:
        logger.error("💥 跨FE节点消息分发测试失败！")
        sys.exit(1)

if __name__ == "__main__":
    main() 