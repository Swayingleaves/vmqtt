#!/usr/bin/env python3
"""
测试跨FE节点消息路由问题：
问题：fe1启动后，fe2启动后，be1启动后，把be1注册到fe1，
     使用client1连上fe1，再使用client2连上fe2，
     client1和client2都订阅topic1，
     client1发布消息到topic1，client2能收到消息，
     但是反过来client2发布消息到topic1，client1收不到消息

@author zhenglin
@mail zhenglin.cn.cq@gmail.com
"""

import paho.mqtt.client as mqtt
import time
import threading
import logging
import sys

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 测试配置
FE1_HOST = "localhost"
FE1_PORT = 1883

FE2_HOST = "localhost" 
FE2_PORT = 1884

TEST_TOPIC = "topic1"

# 全局变量跟踪接收的消息
client1_received_messages = []
client2_received_messages = []
client1_message_event = threading.Event()
client2_message_event = threading.Event()

def on_connect_client1(client, userdata, flags, rc):
    """Client1连接回调"""
    if rc == 0:
        logger.info("✅ Client1连接FE1成功")
        # 立即订阅topic1
        client.subscribe(TEST_TOPIC, qos=1)
        logger.info("📝 Client1订阅topic1")
    else:
        logger.error(f"❌ Client1连接FE1失败，返回码: {rc}")

def on_connect_client2(client, userdata, flags, rc):
    """Client2连接回调"""
    if rc == 0:
        logger.info("✅ Client2连接FE2成功")
        # 立即订阅topic1
        client.subscribe(TEST_TOPIC, qos=1)
        logger.info("📝 Client2订阅topic1")
    else:
        logger.error(f"❌ Client2连接FE2失败，返回码: {rc}")

def on_message_client1(client, userdata, msg):
    """Client1接收消息回调"""
    message = msg.payload.decode('utf-8')
    logger.info(f"📥 Client1收到消息: {message}")
    client1_received_messages.append(message)
    client1_message_event.set()

def on_message_client2(client, userdata, msg):
    """Client2接收消息回调"""
    message = msg.payload.decode('utf-8')
    logger.info(f"📥 Client2收到消息: {message}")
    client2_received_messages.append(message)
    client2_message_event.set()

def on_subscribe_client1(client, userdata, mid, granted_qos):
    """Client1订阅成功回调"""
    logger.info(f"✅ Client1订阅成功，QoS: {granted_qos}")

def on_subscribe_client2(client, userdata, mid, granted_qos):
    """Client2订阅成功回调"""
    logger.info(f"✅ Client2订阅成功，QoS: {granted_qos}")

def on_publish_client1(client, userdata, mid):
    """Client1发布成功回调"""
    logger.info(f"✅ Client1发布消息成功，消息ID: {mid}")

def on_publish_client2(client, userdata, mid):
    """Client2发布成功回调"""
    logger.info(f"✅ Client2发布消息成功，消息ID: {mid}")

def test_cross_fe_message_routing():
    """测试跨FE节点消息路由"""
    logger.info("🚀 开始测试跨FE节点消息路由问题...")
    
    # 清空接收消息列表
    client1_received_messages.clear()
    client2_received_messages.clear()
    client1_message_event.clear()
    client2_message_event.clear()
    
    # 创建Client1，连接到FE1
    logger.info("📱 创建Client1，连接到FE1...")
    client1 = mqtt.Client(client_id="test_client1", clean_session=True)
    client1.on_connect = on_connect_client1
    client1.on_message = on_message_client1
    client1.on_subscribe = on_subscribe_client1
    client1.on_publish = on_publish_client1
    
    # 创建Client2，连接到FE2
    logger.info("📱 创建Client2，连接到FE2...")
    client2 = mqtt.Client(client_id="test_client2", clean_session=True)
    client2.on_connect = on_connect_client2
    client2.on_message = on_message_client2
    client2.on_subscribe = on_subscribe_client2
    client2.on_publish = on_publish_client2
    
    try:
        # 连接客户端
        logger.info("🔗 连接客户端到FE节点...")
        client1.connect(FE1_HOST, FE1_PORT, 60)
        client2.connect(FE2_HOST, FE2_PORT, 60)
        
        # 启动网络循环
        client1.loop_start()
        client2.loop_start()
        
        # 等待连接和订阅完成
        logger.info("⏳ 等待连接和订阅完成...")
        time.sleep(5)
        
        # 测试1：Client1发布消息到topic1，检查Client2是否能收到
        logger.info("\n🧪 测试1：Client1发布消息 -> Client2接收")
        test1_message = "Hello from Client1 to Client2!"
        logger.info(f"📤 Client1发布消息: {test1_message}")
        client1.publish(TEST_TOPIC, test1_message, qos=1)
        
        # 等待消息接收
        logger.info("⏳ 等待Client2接收消息...")
        received = client2_message_event.wait(timeout=10)
        
        if received and test1_message in client2_received_messages:
            logger.info("✅ 测试1成功：Client2收到了Client1发布的消息")
            test1_success = True
        else:
            logger.error("❌ 测试1失败：Client2没有收到Client1发布的消息")
            test1_success = False
        
        # 重置事件
        client2_message_event.clear()
        time.sleep(2)
        
        # 测试2：Client2发布消息到topic1，检查Client1是否能收到
        logger.info("\n🧪 测试2：Client2发布消息 -> Client1接收")
        test2_message = "Hello from Client2 to Client1!"
        logger.info(f"📤 Client2发布消息: {test2_message}")
        client2.publish(TEST_TOPIC, test2_message, qos=1)
        
        # 等待消息接收
        logger.info("⏳ 等待Client1接收消息...")
        received = client1_message_event.wait(timeout=10)
        
        if received and test2_message in client1_received_messages:
            logger.info("✅ 测试2成功：Client1收到了Client2发布的消息")
            test2_success = True
        else:
            logger.error("❌ 测试2失败：Client1没有收到Client2发布的消息")
            test2_success = False
        
        # 总结测试结果
        logger.info("\n📊 测试结果总结：")
        logger.info(f"测试1 (Client1->Client2): {'✅ 成功' if test1_success else '❌ 失败'}")
        logger.info(f"测试2 (Client2->Client1): {'✅ 成功' if test2_success else '❌ 失败'}")
        
        if test1_success and test2_success:
            logger.info("🎉 所有测试通过！跨FE节点消息路由正常工作")
            return True
        elif test1_success and not test2_success:
            logger.error("⚠️  发现问题：Client1->Client2正常，但Client2->Client1失败")
            logger.error("   这正是我们要解决的问题！")
            return False
        else:
            logger.error("💥 测试失败：跨FE节点消息路由存在问题")
            return False
            
    except Exception as e:
        logger.error(f"💥 测试过程中发生异常: {e}")
        return False
        
    finally:
        # 清理资源
        logger.info("🧹 清理资源...")
        try:
            client1.loop_stop()
            client2.loop_stop()
            client1.disconnect()
            client2.disconnect()
        except Exception as e:
            logger.warning(f"清理资源时发生异常: {e}")

def main():
    """主函数"""
    logger.info("="*60)
    logger.info("跨FE节点消息路由问题测试")
    logger.info("="*60)
    
    success = test_cross_fe_message_routing()
    
    if success:
        logger.info("🎉 测试完成：问题已解决")
        sys.exit(0)
    else:
        logger.error("❌ 测试完成：问题仍然存在")
        sys.exit(1)

if __name__ == "__main__":
    main() 