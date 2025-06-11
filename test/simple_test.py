#!/usr/bin/env python3

"""
简单的跨FE节点通信测试
"""

import paho.mqtt.client as mqtt
import time
import threading

# 全局变量
received_messages = []
message_event = threading.Event()

def on_connect_client1(client, userdata, flags, rc):
    print(f"Client1 connected with result code {rc}")

def on_connect_client2(client, userdata, flags, rc):
    print(f"Client2 connected with result code {rc}")

def on_message_client2(client, userdata, msg):
    message = msg.payload.decode()
    print(f"Client2 received: {message}")
    received_messages.append(message)
    message_event.set()

def main():
    print("=== 简单跨FE节点通信测试 ===")
    
    # 创建Client1连接FE1
    client1 = mqtt.Client(client_id="simple_client1", clean_session=True)
    client1.on_connect = on_connect_client1
    
    # 创建Client2连接FE2  
    client2 = mqtt.Client(client_id="simple_client2", clean_session=True)
    client2.on_connect = on_connect_client2
    client2.on_message = on_message_client2
    
    try:
        # 连接客户端
        print("1. 连接Client1到FE1...")
        client1.connect("localhost", 1883, 60)
        client1.loop_start()
        time.sleep(2)
        
        print("2. 连接Client2到FE2...")  
        client2.connect("localhost", 1884, 60)
        client2.loop_start()
        time.sleep(2)
        
        # Client2订阅
        print("3. Client2订阅topic 'simple_test'...")
        client2.subscribe("simple_test", 1)
        time.sleep(3)  # 等待订阅同步
        
        # Client1发布消息
        print("4. Client1发布消息...")
        client1.publish("simple_test", "Hello from Client1!", qos=1)
        time.sleep(2)
        
        # 等待消息
        print("5. 等待消息接收...")
        if message_event.wait(timeout=10):
            print("✅ 测试成功！Client2收到了消息")
            print(f"   收到的消息: {received_messages}")
        else:
            print("❌ 测试失败！Client2没有收到消息")
            
    except Exception as e:
        print(f"测试异常: {e}")
    finally:
        client1.disconnect()
        client2.disconnect()
        client1.loop_stop()
        client2.loop_stop()

if __name__ == "__main__":
    main() 