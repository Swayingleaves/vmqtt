#!/usr/bin/env python3
"""
跨FE节点消息分发测试
测试FE1和FE2之间的消息分发功能
"""
import paho.mqtt.client as mqtt
import time
import threading
import sys

# 全局变量
messages_received = []
message_event = threading.Event()

def on_connect(client, userdata, flags, rc):
    """连接回调"""
    if rc == 0:
        print(f"[{userdata['name']}] 连接成功")
    else:
        print(f"[{userdata['name']}] 连接失败，错误码: {rc}")

def on_message(client, userdata, msg):
    """消息接收回调"""
    message = {
        'client': userdata['name'],
        'topic': msg.topic,
        'payload': msg.payload.decode('utf-8'),
        'timestamp': time.time()
    }
    messages_received.append(message)
    print(f"[{userdata['name']}] 收到消息: {message}")
    message_event.set()

def on_subscribe(client, userdata, mid, granted_qos):
    """订阅成功回调"""
    print(f"[{userdata['name']}] 订阅成功, QoS: {granted_qos}")

def on_publish(client, userdata, mid):
    """发布成功回调"""
    print(f"[{userdata['name']}] 消息发布成功, mid: {mid}")

def main():
    print("开始跨FE节点消息分发测试...")
    
    # 清空接收消息列表
    messages_received.clear()
    message_event.clear()
    
    # 创建客户端1，连接到FE1
    client1 = mqtt.Client(client_id="test_client1")
    client1.user_data_set({'name': 'Client1-FE1'})
    client1.on_connect = on_connect
    client1.on_message = on_message
    client1.on_subscribe = on_subscribe
    client1.on_publish = on_publish
    
    # 创建客户端2，连接到FE2
    client2 = mqtt.Client(client_id="test_client2")
    client2.user_data_set({'name': 'Client2-FE2'})
    client2.on_connect = on_connect
    client2.on_message = on_message
    client2.on_subscribe = on_subscribe
    client2.on_publish = on_publish
    
    try:
        # 连接到FE节点
        print("正在连接到FE节点...")
        client1.connect("localhost", 1883, 60)
        client2.connect("localhost", 1884, 60)
        
        # 启动网络循环
        client1.loop_start()
        client2.loop_start()
        
        # 等待连接建立
        time.sleep(2)
        
        # Client2订阅topic1
        print("Client2 订阅 topic1...")
        client2.subscribe("topic1", qos=1)
        
        # 等待订阅完成和同步
        time.sleep(3)
        
        # Client1发布消息到topic1
        test_message = "Hello from Client1 to Client2 via cross-FE!"
        print(f"Client1 发布消息: {test_message}")
        client1.publish("topic1", test_message, qos=1)
        
        # 等待消息接收
        print("等待消息接收...")
        received = message_event.wait(timeout=10)
        
        if received:
            print("\n测试结果分析:")
            print(f"总共收到 {len(messages_received)} 条消息")
            
            for i, msg in enumerate(messages_received):
                print(f"  消息{i+1}: {msg}")
            
            # 检查是否有Client2收到的消息
            client2_messages = [msg for msg in messages_received if 'Client2' in msg['client']]
            
            if client2_messages:
                print("\n✅ 测试成功！Client2成功收到来自Client1的消息")
                print("跨FE节点消息分发功能正常工作")
                return True
            else:
                print("\n❌ 测试失败！Client2没有收到来自Client1的消息")
                print("跨FE节点消息分发功能存在问题")
                return False
        else:
            print("\n❌ 测试超时！在10秒内没有收到任何消息")
            return False
            
    except Exception as e:
        print(f"测试过程中发生错误: {e}")
        return False
        
    finally:
        # 清理资源
        print("\n清理测试资源...")
        try:
            client1.loop_stop()
            client2.loop_stop()
            client1.disconnect()
            client2.disconnect()
        except:
            pass

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 