#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试QoS 0消息的具体问题：
client1连接到FE1并订阅topic/test (QoS 0)，
client2连接FE2并订阅topic/test (QoS 0)，
client1发送QoS 0消息到topic/test，但是client2收不到，
但是反过来client2发送QoS 0消息client1可以收到

@author zhenglin
@mail zhenglin.cn.cq@gmail.com
"""

import paho.mqtt.client as mqtt
import threading
import time
import sys
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/tmp/test_qos0_problem.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# 测试配置
FE1_HOST = "localhost"
FE1_PORT = 1883
FE2_HOST = "localhost"
FE2_PORT = 1884
TEST_TOPIC = "topic/test"

# 全局变量记录测试结果
client1_received_messages = []
client2_received_messages = []
test_lock = threading.Lock()

class MQTTTestClient:
    def __init__(self, client_id, host, port, receive_list):
        self.client_id = client_id
        self.host = host
        self.port = port
        self.receive_list = receive_list
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
        self.connected = False
        self.subscribed = False
        
        # 设置回调
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.client.on_subscribe = self.on_subscribe
        self.client.on_publish = self.on_publish
        
    def on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            self.connected = True
            logger.info(f"[{self.client_id}] 连接成功到 {self.host}:{self.port}")
        else:
            logger.error(f"[{self.client_id}] 连接失败: {reason_code}")
            
    def on_disconnect(self, client, userdata, reason_code, properties=None):
        self.connected = False
        self.subscribed = False
        logger.info(f"[{self.client_id}] 连接断开: {reason_code}")
        
    def on_message(self, client, userdata, msg):
        message = {
            'topic': msg.topic,
            'payload': msg.payload.decode(),
            'timestamp': time.time(),
            'client': self.client_id,
            'qos': msg.qos
        }
        
        with test_lock:
            self.receive_list.append(message)
            
        logger.info(f"[{self.client_id}] 收到QoS {msg.qos}消息: topic={msg.topic}, payload={msg.payload.decode()}")
        
    def on_subscribe(self, client, userdata, mid, reason_codes, properties=None):
        # 检查订阅是否成功
        try:
            if hasattr(reason_codes, '__iter__'):
                # 处理列表或元组形式的reason_codes
                success = all(self._check_reason_code(rc) for rc in reason_codes)
            else:
                # 处理单个reason_code
                success = self._check_reason_code(reason_codes)
                
            if success:
                self.subscribed = True
                logger.info(f"[{self.client_id}] QoS 0订阅成功: topic={TEST_TOPIC}, codes={reason_codes}")
            else:
                logger.error(f"[{self.client_id}] QoS 0订阅失败: {reason_codes}")
        except Exception as e:
            logger.error(f"[{self.client_id}] 订阅回调异常: {e}, codes={reason_codes}")
            # 为了测试，假设订阅成功
            self.subscribed = True
    
    def _check_reason_code(self, code):
        """检查reason code是否表示成功"""
        # 处理不同类型的reason code
        if hasattr(code, 'value'):
            # ReasonCode对象
            return code.value in [0, 1, 2]
        elif isinstance(code, int):
            # 整数值：0=QoS0成功, 1=QoS1成功, 2=QoS2成功
            return code in [0, 1, 2]
        else:
            # 其他情况，尝试转换为字符串检查
            code_str = str(code).lower()
            return 'granted' in code_str or 'success' in code_str
            
    def on_publish(self, client, userdata, mid, reason_code=None, properties=None):
        # MQTT v3.1.1只有mid参数，v5有reason_code
        if reason_code is None or reason_code == 0:
            logger.info(f"[{self.client_id}] QoS 0发布成功: mid={mid}")
        else:
            logger.error(f"[{self.client_id}] QoS 0发布失败: {reason_code}")
    
    def connect(self):
        try:
            self.client.connect(self.host, self.port, 60)
            self.client.loop_start()
            
            # 等待连接成功
            for i in range(10):
                if self.connected:
                    return True
                time.sleep(0.5)
                
            logger.error(f"[{self.client_id}] 连接超时")
            return False
            
        except Exception as e:
            logger.error(f"[{self.client_id}] 连接异常: {e}")
            return False
    
    def subscribe(self, topic):
        if not self.connected:
            logger.error(f"[{self.client_id}] 未连接，无法订阅")
            return False
            
        try:
            # 使用QoS 0订阅
            result = self.client.subscribe(topic, qos=0)
            logger.info(f"[{self.client_id}] 发送QoS 0订阅请求: topic={topic}, result={result}")
            
            # 等待订阅成功
            for i in range(10):
                if self.subscribed:
                    return True
                time.sleep(0.5)
                
            logger.error(f"[{self.client_id}] QoS 0订阅超时")
            return False
            
        except Exception as e:
            logger.error(f"[{self.client_id}] QoS 0订阅异常: {e}")
            return False
    
    def publish(self, topic, payload):
        if not self.connected:
            logger.error(f"[{self.client_id}] 未连接，无法发布")
            return False
            
        try:
            # 使用QoS 0发布
            result = self.client.publish(topic, payload, qos=0)
            logger.info(f"[{self.client_id}] 发送QoS 0发布请求: topic={topic}, payload={payload}, result={result}")
            return True
            
        except Exception as e:
            logger.error(f"[{self.client_id}] QoS 0发布异常: {e}")
            return False
    
    def disconnect(self):
        try:
            self.client.loop_stop()
            self.client.disconnect()
            logger.info(f"[{self.client_id}] 断开连接")
        except Exception as e:
            logger.error(f"[{self.client_id}] 断开连接异常: {e}")

def test_qos0_problem():
    """测试QoS 0消息的具体问题：client1->client2失败，但client2->client1成功"""
    logger.info("========================================")
    logger.info("测试QoS 0消息的具体问题场景")
    logger.info("FE1: localhost:1883, FE2: localhost:1884")
    logger.info("Topic: topic/test (QoS 0)")
    logger.info("========================================")
    
    # 清空接收消息列表
    global client1_received_messages, client2_received_messages
    client1_received_messages.clear()
    client2_received_messages.clear()
    
    # 创建客户端（模拟mqttx客户端ID格式）
    client1 = MQTTTestClient("mqttx_test_client1", FE1_HOST, FE1_PORT, client1_received_messages)
    client2 = MQTTTestClient("mqttx_test_client2", FE2_HOST, FE2_PORT, client2_received_messages)
    
    try:
        # 步骤1: 连接客户端
        logger.info("步骤1: 连接客户端")
        if not client1.connect():
            logger.error("Client1连接失败")
            return False
            
        if not client2.connect():
            logger.error("Client2连接失败")
            return False
            
        time.sleep(2)  # 等待连接稳定
        
        # 步骤2: 使用QoS 0订阅主题
        logger.info(f"步骤2: 两个客户端都使用QoS 0订阅 {TEST_TOPIC}")
        if not client1.subscribe(TEST_TOPIC):
            logger.error("Client1 QoS 0订阅失败")
            return False
            
        if not client2.subscribe(TEST_TOPIC):
            logger.error("Client2 QoS 0订阅失败")
            return False
            
        time.sleep(5)  # 给更多时间等待订阅同步
        
        logger.info("步骤3: 检查当前订阅状态")
        logger.info(f"Client1已订阅: {client1.subscribed}")
        logger.info(f"Client2已订阅: {client2.subscribed}")
        
        # 步骤4: 测试Client1 -> Client2消息传输（QoS 0，这是问题场景）
        logger.info("========================================")
        logger.info("步骤4: 测试Client1 -> Client2 QoS 0消息传输（问题场景）")
        logger.info("========================================")
        test_message_1 = "Hello from Client1 QoS0 message"
        
        if not client1.publish(TEST_TOPIC, test_message_1):
            logger.error("Client1发布QoS 0消息失败")
            return False
            
        time.sleep(8)  # QoS 0消息可能需要更多时间传输
        
        # 验证Client2是否收到消息
        client2_received = False
        with test_lock:
            for msg in client2_received_messages:
                if msg['payload'] == test_message_1:
                    client2_received = True
                    break
                    
        if client2_received:
            logger.info("✅ Client1 -> Client2: QoS 0消息传输成功")
        else:
            logger.error("❌ Client1 -> Client2: QoS 0消息传输失败（这是报告的问题）")
            logger.error(f"Client2收到的消息: {client2_received_messages}")
            
        # 步骤5: 测试Client2 -> Client1消息传输（QoS 0，这应该成功）
        logger.info("========================================")
        logger.info("步骤5: 测试Client2 -> Client1 QoS 0消息传输（应该成功）")
        logger.info("========================================")
        test_message_2 = "Hello from Client2 QoS0 message"
        
        if not client2.publish(TEST_TOPIC, test_message_2):
            logger.error("Client2发布QoS 0消息失败")
            return False
            
        time.sleep(8)  # QoS 0消息可能需要更多时间传输
        
        # 验证Client1是否收到消息
        client1_received = False
        with test_lock:
            for msg in client1_received_messages:
                if msg['payload'] == test_message_2:
                    client1_received = True
                    break
                    
        if client1_received:
            logger.info("✅ Client2 -> Client1: QoS 0消息传输成功")
        else:
            logger.error("❌ Client2 -> Client1: QoS 0消息传输失败")
            logger.error(f"Client1收到的消息: {client1_received_messages}")
        
        # 测试结果总结
        logger.info("========================================")
        logger.info("QoS 0消息测试结果总结:")
        logger.info(f"Client1 -> Client2: {'成功' if client2_received else '失败 ❌'}")
        logger.info(f"Client2 -> Client1: {'成功' if client1_received else '失败'}")
        logger.info(f"Client1收到消息数: {len(client1_received_messages)}")
        logger.info(f"Client2收到消息数: {len(client2_received_messages)}")
        
        if client2_received and client1_received:
            logger.info("问题已解决：QoS 0双向通信都正常")
        elif not client2_received and client1_received:
            logger.error("问题确认：Client1->Client2的QoS 0消息失败，但Client2->Client1成功")
        elif client2_received and not client1_received:
            logger.error("相反问题：Client2->Client1的QoS 0消息失败，但Client1->Client2成功") 
        else:
            logger.error("严重问题：QoS 0双向通信都失败")
            
        logger.info("========================================")
        
        # 显示所有收到的消息详情
        logger.info("Client1收到的所有QoS 0消息:")
        for i, msg in enumerate(client1_received_messages):
            logger.info(f"  {i+1}. topic={msg['topic']}, payload={msg['payload']}, qos={msg['qos']}")
            
        logger.info("Client2收到的所有QoS 0消息:")
        for i, msg in enumerate(client2_received_messages):
            logger.info(f"  {i+1}. topic={msg['topic']}, payload={msg['payload']}, qos={msg['qos']}")
        
        return client2_received and client1_received
        
    except Exception as e:
        logger.error(f"测试过程中发生异常: {e}")
        return False
        
    finally:
        # 清理资源
        client1.disconnect()
        client2.disconnect()
        time.sleep(1)

def main():
    """主函数"""
    logger.info("开始测试QoS 0消息的具体问题场景")
    
    # 运行测试
    success = test_qos0_problem()
    
    if success:
        logger.info("🎉 QoS 0问题已解决！双向消息传输正常工作")
        sys.exit(0)
    else:
        logger.error("💥 QoS 0问题确认存在！需要进一步调试")
        sys.exit(1)

if __name__ == "__main__":
    main() 