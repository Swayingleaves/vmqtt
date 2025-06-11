#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
模拟mqttx的测试场景：
更接近真实的mqttx客户端行为测试

@author zhenglin
@mail zhenglin.cn.cq@gmail.com
"""

import paho.mqtt.client as mqtt
import threading
import time
import sys
import logging
import json

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/tmp/test_mqttx_scenario.log'),
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
results = {
    'client1_messages': [],
    'client2_messages': [],
    'test_lock': threading.Lock()
}

class MQTTXClient:
    """模拟MQTTX客户端行为"""
    
    def __init__(self, client_id, host, port):
        self.client_id = client_id
        self.host = host
        self.port = port
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
        self.connected = False
        self.subscribed = False
        self.received_messages = []
        
        # 设置回调
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.client.on_subscribe = self.on_subscribe
        self.client.on_publish = self.on_publish
        self.client.on_log = self.on_log
        
    def on_log(self, client, userdata, level, buf):
        logger.debug(f"[{self.client_id}] MQTT日志: {buf}")
        
    def on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            self.connected = True
            logger.info(f"[{self.client_id}] ✅ 连接成功到 {self.host}:{self.port}")
            logger.info(f"[{self.client_id}] 连接标志: {flags}")
        else:
            logger.error(f"[{self.client_id}] ❌ 连接失败: {reason_code}")
            
    def on_disconnect(self, client, userdata, reason_code, properties=None):
        self.connected = False
        self.subscribed = False
        logger.info(f"[{self.client_id}] 连接断开: {reason_code}")
        
    def on_message(self, client, userdata, msg):
        message_info = {
            'topic': msg.topic,
            'payload': msg.payload.decode(),
            'qos': msg.qos,
            'retain': msg.retain,
            'timestamp': time.time(),
            'client_receiver': self.client_id
        }
        
        with results['test_lock']:
            self.received_messages.append(message_info)
            if self.client_id == 'mqttx_client1':
                results['client1_messages'].append(message_info)
            elif self.client_id == 'mqttx_client2':
                results['client2_messages'].append(message_info)
            
        logger.info(f"[{self.client_id}] 📨 收到消息: topic={msg.topic}, payload={msg.payload.decode()}, qos={msg.qos}, retain={msg.retain}")
        
    def on_subscribe(self, client, userdata, mid, reason_codes, properties=None):
        logger.info(f"[{self.client_id}] 📋 订阅回调: mid={mid}, reason_codes={reason_codes}")
        try:
            if hasattr(reason_codes, '__iter__'):
                success = all(self._is_success_code(rc) for rc in reason_codes)
            else:
                success = self._is_success_code(reason_codes)
                
            if success:
                self.subscribed = True
                logger.info(f"[{self.client_id}] ✅ 订阅成功: topic={TEST_TOPIC}")
            else:
                logger.error(f"[{self.client_id}] ❌ 订阅失败: {reason_codes}")
        except Exception as e:
            logger.error(f"[{self.client_id}] 订阅回调异常: {e}")
            self.subscribed = True  # 假设成功继续测试
    
    def _is_success_code(self, code):
        """检查订阅是否成功"""
        if hasattr(code, 'value'):
            return code.value in [0, 1, 2]
        elif isinstance(code, int):
            return code in [0, 1, 2]
        else:
            code_str = str(code).lower()
            return 'granted' in code_str or code_str in ['0', '1', '2']
            
    def on_publish(self, client, userdata, mid, reason_code=None, properties=None):
        if reason_code is None or reason_code == 0:
            logger.info(f"[{self.client_id}] ✅ 发布成功: mid={mid}")
        else:
            logger.error(f"[{self.client_id}] ❌ 发布失败: {reason_code}")
    
    def connect_and_wait(self, timeout=10):
        """连接并等待成功"""
        try:
            logger.info(f"[{self.client_id}] 🔗 正在连接到 {self.host}:{self.port}...")
            self.client.connect(self.host, self.port, 60)
            self.client.loop_start()
            
            # 等待连接成功
            start_time = time.time()
            while not self.connected and (time.time() - start_time) < timeout:
                time.sleep(0.1)
                
            if self.connected:
                logger.info(f"[{self.client_id}] ✅ 连接建立完成")
                return True
            else:
                logger.error(f"[{self.client_id}] ❌ 连接超时")
                return False
                
        except Exception as e:
            logger.error(f"[{self.client_id}] 连接异常: {e}")
            return False
    
    def subscribe_and_wait(self, topic, qos=0, timeout=10):
        """订阅并等待成功"""
        if not self.connected:
            logger.error(f"[{self.client_id}] 未连接，无法订阅")
            return False
            
        try:
            logger.info(f"[{self.client_id}] 📋 发起订阅: topic={topic}, qos={qos}")
            result = self.client.subscribe(topic, qos=qos)
            logger.info(f"[{self.client_id}] 订阅请求结果: {result}")
            
            # 等待订阅成功
            start_time = time.time()
            while not self.subscribed and (time.time() - start_time) < timeout:
                time.sleep(0.1)
                
            if self.subscribed:
                logger.info(f"[{self.client_id}] ✅ 订阅完成")
                return True
            else:
                logger.error(f"[{self.client_id}] ❌ 订阅超时")
                return False
                
        except Exception as e:
            logger.error(f"[{self.client_id}] 订阅异常: {e}")
            return False
    
    def publish_message(self, topic, payload, qos=0, retain=False):
        """发布消息"""
        if not self.connected:
            logger.error(f"[{self.client_id}] 未连接，无法发布")
            return False
            
        try:
            logger.info(f"[{self.client_id}] 📤 发布消息: topic={topic}, payload={payload}, qos={qos}, retain={retain}")
            result = self.client.publish(topic, payload, qos=qos, retain=retain)
            logger.info(f"[{self.client_id}] 发布请求结果: {result}")
            return True
            
        except Exception as e:
            logger.error(f"[{self.client_id}] 发布异常: {e}")
            return False
    
    def disconnect_client(self):
        """断开连接"""
        try:
            if self.connected:
                self.client.loop_stop()
                self.client.disconnect()
                logger.info(f"[{self.client_id}] 🔌 断开连接")
            else:
                logger.info(f"[{self.client_id}] 已经断开连接")
        except Exception as e:
            logger.error(f"[{self.client_id}] 断开连接异常: {e}")

def wait_for_messages(expected_count, timeout=15):
    """等待消息接收"""
    logger.info(f"⏳ 等待消息传输完成，预期消息数：{expected_count}，超时：{timeout}秒")
    
    start_time = time.time()
    while (time.time() - start_time) < timeout:
        with results['test_lock']:
            total_messages = len(results['client1_messages']) + len(results['client2_messages'])
            if total_messages >= expected_count:
                logger.info(f"✅ 消息传输完成，总消息数：{total_messages}")
                return True
        time.sleep(0.5)
    
    with results['test_lock']:
        total_messages = len(results['client1_messages']) + len(results['client2_messages'])
        logger.warning(f"⚠️ 等待消息超时，当前消息数：{total_messages}")
        return False

def test_mqttx_scenario():
    """模拟MQTTX测试场景"""
    logger.info("=" * 50)
    logger.info("🧪 开始MQTTX场景测试")
    logger.info("📋 测试配置:")
    logger.info(f"   FE1: {FE1_HOST}:{FE1_PORT}")
    logger.info(f"   FE2: {FE2_HOST}:{FE2_PORT}")
    logger.info(f"   Topic: {TEST_TOPIC}")
    logger.info("=" * 50)
    
    # 清空消息记录
    with results['test_lock']:
        results['client1_messages'].clear()
        results['client2_messages'].clear()
    
    # 创建两个客户端
    client1 = MQTTXClient("mqttx_client1", FE1_HOST, FE1_PORT)
    client2 = MQTTXClient("mqttx_client2", FE2_HOST, FE2_PORT)
    
    try:
        # 步骤1: 连接客户端
        logger.info("\n🔗 步骤1: 连接客户端到不同FE节点")
        if not client1.connect_and_wait():
            logger.error("❌ Client1连接失败")
            return False
            
        if not client2.connect_and_wait():
            logger.error("❌ Client2连接失败")
            return False
            
        logger.info("✅ 两个客户端连接成功")
        
        # 步骤2: 订阅同一个topic
        logger.info(f"\n📋 步骤2: 两个客户端订阅相同topic (QoS 0)")
        if not client1.subscribe_and_wait(TEST_TOPIC, qos=0):
            logger.error("❌ Client1订阅失败")
            return False
            
        if not client2.subscribe_and_wait(TEST_TOPIC, qos=0):
            logger.error("❌ Client2订阅失败")
            return False
            
        logger.info("✅ 两个客户端订阅成功")
        
        # 等待订阅信息同步
        logger.info("\n⏳ 等待订阅信息在FE集群间同步...")
        time.sleep(5)
        
        # 步骤3: Client1发布消息给Client2（这是问题场景）
        logger.info(f"\n📤 步骤3: Client1发布消息测试 (FE1 -> FE2)")
        test_message_1 = "MQTTX Test Message from Client1"
        if not client1.publish_message(TEST_TOPIC, test_message_1, qos=0):
            logger.error("❌ Client1发布消息失败")
            return False
        
        # 等待消息传输
        logger.info("⏳ 等待消息从Client1传输到Client2...")
        wait_for_messages(2, timeout=10)  # 期望2条消息（发送者自己收到1条，接收者收到1条）
        
        # 检查Client2是否收到消息
        client2_received_msg1 = False
        with results['test_lock']:
            for msg in results['client2_messages']:
                if msg['payload'] == test_message_1:
                    client2_received_msg1 = True
                    logger.info(f"✅ Client2收到来自Client1的消息: {msg['payload']}")
                    break
        
        if not client2_received_msg1:
            logger.error(f"❌ Client2未收到来自Client1的消息")
            logger.error(f"Client2收到的消息: {results['client2_messages']}")
        
        # 步骤4: Client2发布消息给Client1（反向测试）
        logger.info(f"\n📤 步骤4: Client2发布消息测试 (FE2 -> FE1)")
        test_message_2 = "MQTTX Test Message from Client2"
        if not client2.publish_message(TEST_TOPIC, test_message_2, qos=0):
            logger.error("❌ Client2发布消息失败")
            return False
        
        # 等待消息传输
        logger.info("⏳ 等待消息从Client2传输到Client1...")
        wait_for_messages(4, timeout=10)  # 期望总共4条消息
        
        # 检查Client1是否收到消息
        client1_received_msg2 = False
        with results['test_lock']:
            for msg in results['client1_messages']:
                if msg['payload'] == test_message_2:
                    client1_received_msg2 = True
                    logger.info(f"✅ Client1收到来自Client2的消息: {msg['payload']}")
                    break
        
        if not client1_received_msg2:
            logger.error(f"❌ Client1未收到来自Client2的消息")
            logger.error(f"Client1收到的消息: {results['client1_messages']}")
        
        # 结果分析
        logger.info("\n" + "=" * 50)
        logger.info("📊 测试结果分析:")
        logger.info(f"Client1 -> Client2: {'✅ 成功' if client2_received_msg1 else '❌ 失败'}")
        logger.info(f"Client2 -> Client1: {'✅ 成功' if client1_received_msg2 else '❌ 失败'}")
        
        with results['test_lock']:
            logger.info(f"Client1总消息数: {len(results['client1_messages'])}")
            logger.info(f"Client2总消息数: {len(results['client2_messages'])}")
        
        # 详细消息记录
        logger.info("\n📝 详细消息记录:")
        with results['test_lock']:
            logger.info("Client1收到的消息:")
            for i, msg in enumerate(results['client1_messages'], 1):
                logger.info(f"  {i}. {msg['payload']} (qos={msg['qos']}, 时间={msg['timestamp']:.3f})")
                
            logger.info("Client2收到的消息:")
            for i, msg in enumerate(results['client2_messages'], 1):
                logger.info(f"  {i}. {msg['payload']} (qos={msg['qos']}, 时间={msg['timestamp']:.3f})")
        
        success = client2_received_msg1 and client1_received_msg2
        
        if success:
            logger.info("\n🎉 测试通过：双向消息传输正常")
        else:
            logger.error("\n💥 测试失败：存在消息传输问题")
            if not client2_received_msg1:
                logger.error("   - Client1 -> Client2 消息传输失败")
            if not client1_received_msg2:
                logger.error("   - Client2 -> Client1 消息传输失败")
        
        logger.info("=" * 50)
        return success
        
    except Exception as e:
        logger.error(f"测试过程中发生异常: {e}", exc_info=True)
        return False
        
    finally:
        # 清理资源
        logger.info("\n🧹 清理资源...")
        client1.disconnect_client()
        client2.disconnect_client()
        time.sleep(1)

def main():
    """主函数"""
    success = test_mqttx_scenario()
    
    if success:
        logger.info("🎉 MQTTX场景测试通过")
        sys.exit(0)
    else:
        logger.error("💥 MQTTX场景测试失败")
        sys.exit(1)

if __name__ == "__main__":
    main() 