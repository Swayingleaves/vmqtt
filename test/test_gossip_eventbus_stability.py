#!/usr/bin/env python3
"""
测试Gossip协议和EventBusServer稳定性优化效果
验证连接重置和心跳超时问题是否得到解决

@author zhenglin
@mail zhenglin.cn.cq@gmail.com
"""

import time
import requests
import subprocess
import signal
import os
import sys
import json
import threading
from datetime import datetime

class StabilityTester:
    def __init__(self):
        self.fe_processes = []
        self.be_processes = []
        self.test_results = {
            'gossip_errors': 0,
            'eventbus_errors': 0,
            'connection_resets': 0,
            'heartbeat_timeouts': 0,
            'total_messages': 0,
            'successful_connections': 0,
            'failed_connections': 0
        }
        
    def start_fe_node(self, config_file):
        """启动FE节点"""
        try:
            print(f"启动FE节点: {config_file}")
            process = subprocess.Popen(
                ['./start-fe.sh', '--config', config_file],
                cwd='../output',
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            self.fe_processes.append(process)
            time.sleep(3)  # 等待启动
            return process
        except Exception as e:
            print(f"启动FE节点失败: {e}")
            return None
    
    def start_be_node(self, config_file):
        """启动BE节点"""
        try:
            print(f"启动BE节点: {config_file}")
            process = subprocess.Popen(
                ['./start-be.sh', '--config', config_file],
                cwd='../output',
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            self.be_processes.append(process)
            time.sleep(3)  # 等待启动
            return process
        except Exception as e:
            print(f"启动BE节点失败: {e}")
            return None
    
    def stop_all_processes(self):
        """停止所有进程"""
        print("停止所有FE和BE进程...")
        
        # 停止FE进程
        try:
            subprocess.run(['./stop-fe.sh'], cwd='../output', timeout=10)
        except:
            pass
        
        # 停止BE进程
        try:
            subprocess.run(['./stop-be.sh'], cwd='../output', timeout=10)
        except:
            pass
        
        # 强制终止进程
        for process in self.fe_processes + self.be_processes:
            try:
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=5)
            except:
                try:
                    process.kill()
                except:
                    pass
        
        self.fe_processes.clear()
        self.be_processes.clear()
        time.sleep(2)
    
    def register_be_to_fe(self, fe_port=8081):
        """注册BE节点到FE"""
        try:
            url = f"http://localhost:{fe_port}/api/be/nodes"
            data = {
                "nodeId": "be-node-1",
                "host": "localhost",
                "port": 18080,
                "grpcPort": 19090
            }
            
            response = requests.post(url, json=data, timeout=10)
            if response.status_code == 200:
                print("BE节点注册成功")
                self.test_results['successful_connections'] += 1
                return True
            else:
                print(f"BE节点注册失败: {response.status_code}")
                self.test_results['failed_connections'] += 1
                return False
        except Exception as e:
            print(f"BE节点注册异常: {e}")
            self.test_results['failed_connections'] += 1
            return False
    
    def get_fe_stats(self, fe_port):
        """获取FE节点统计信息"""
        try:
            response = requests.get(f"http://localhost:{fe_port}/api/fe/stats", timeout=5)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"获取FE统计信息失败 (端口{fe_port}): {e}")
            return None
    
    def get_gossip_stats(self, fe_port):
        """获取Gossip统计信息"""
        try:
            response = requests.get(f"http://localhost:{fe_port}/api/fe/cluster", timeout=5)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"获取Gossip统计信息失败 (端口{fe_port}): {e}")
            return None
    
    def monitor_logs(self, duration=60):
        """监控日志文件，统计错误"""
        print(f"开始监控日志 {duration} 秒...")
        
        def count_errors_in_file(filepath, error_patterns):
            """统计文件中的错误"""
            if not os.path.exists(filepath):
                return
            
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
                    
                for pattern, counter_key in error_patterns.items():
                    count = content.count(pattern)
                    self.test_results[counter_key] += count
                    if count > 0:
                        print(f"检测到 {count} 个 '{pattern}' 错误")
            except Exception as e:
                print(f"读取日志文件失败 {filepath}: {e}")
        
        # 错误模式映射
        error_patterns = {
            'java.net.SocketException: Connection reset': 'connection_resets',
            '节点疑似故障': 'heartbeat_timeouts',
            'Gossip消息失败': 'gossip_errors',
            'EventBus': 'eventbus_errors'
        }
        
        # 监控日志文件
        log_files = [
            '../output/logs/fe/fe-server.log',
            '../output/logs/be/be-server.log'
        ]
        
        start_time = time.time()
        while time.time() - start_time < duration:
            for log_file in log_files:
                count_errors_in_file(log_file, error_patterns)
            time.sleep(5)  # 每5秒检查一次
    
    def stress_test_connections(self, duration=30):
        """连接压力测试"""
        print(f"开始连接压力测试 {duration} 秒...")
        
        def stress_worker():
            """压力测试工作线程"""
            start_time = time.time()
            while time.time() - start_time < duration:
                try:
                    # 获取FE统计信息
                    stats1 = self.get_fe_stats(8081)
                    stats2 = self.get_fe_stats(8082)
                    
                    if stats1:
                        self.test_results['total_messages'] += stats1.get('totalMessages', 0)
                    if stats2:
                        self.test_results['total_messages'] += stats2.get('totalMessages', 0)
                    
                    # 获取Gossip统计信息
                    gossip1 = self.get_gossip_stats(8081)
                    gossip2 = self.get_gossip_stats(8082)
                    
                    time.sleep(2)
                except Exception as e:
                    print(f"压力测试异常: {e}")
                    self.test_results['failed_connections'] += 1
        
        # 启动多个压力测试线程
        threads = []
        for i in range(3):
            thread = threading.Thread(target=stress_worker)
            thread.daemon = True
            thread.start()
            threads.append(thread)
        
        # 等待测试完成
        for thread in threads:
            thread.join()
    
    def test_network_interruption(self):
        """测试网络中断恢复"""
        print("测试网络中断恢复...")
        
        # 模拟网络中断（通过重启一个FE节点）
        print("模拟网络中断：重启FE2节点")
        
        # 停止FE2
        try:
            subprocess.run(['pkill', '-f', 'fe2-config.yml'], timeout=5)
        except:
            pass
        
        time.sleep(5)
        
        # 重启FE2
        self.start_fe_node('fe2-config.yml')
        time.sleep(10)
        
        # 检查恢复情况
        stats = self.get_gossip_stats(8081)
        if stats and len(stats.get('clusterNodes', [])) >= 2:
            print("网络中断恢复测试通过")
            self.test_results['successful_connections'] += 1
        else:
            print("网络中断恢复测试失败")
            self.test_results['failed_connections'] += 1
    
    def run_stability_test(self):
        """运行完整的稳定性测试"""
        print("=" * 60)
        print("开始Gossip协议和EventBusServer稳定性测试")
        print("=" * 60)
        
        try:
            # 1. 停止所有现有进程
            self.stop_all_processes()
            
            # 2. 启动FE节点
            print("\n1. 启动FE节点...")
            fe1 = self.start_fe_node('fe1-config.yml')
            fe2 = self.start_fe_node('fe2-config.yml')
            
            if not fe1 or not fe2:
                print("FE节点启动失败")
                return False
            
            # 3. 启动BE节点
            print("\n2. 启动BE节点...")
            be1 = self.start_be_node('be1-config.yml')
            
            if not be1:
                print("BE节点启动失败")
                return False
            
            # 4. 注册BE到FE
            print("\n3. 注册BE节点到FE...")
            time.sleep(5)  # 等待服务完全启动
            if not self.register_be_to_fe():
                print("BE节点注册失败")
                return False
            
            # 5. 等待集群稳定
            print("\n4. 等待集群稳定...")
            time.sleep(10)
            
            # 6. 开始监控和测试
            print("\n5. 开始稳定性测试...")
            
            # 并行执行监控和压力测试
            monitor_thread = threading.Thread(target=self.monitor_logs, args=(120,))
            stress_thread = threading.Thread(target=self.stress_test_connections, args=(60,))
            
            monitor_thread.start()
            stress_thread.start()
            
            # 等待一段时间后进行网络中断测试
            time.sleep(30)
            self.test_network_interruption()
            
            # 等待所有测试完成
            stress_thread.join()
            monitor_thread.join()
            
            # 7. 输出测试结果
            self.print_test_results()
            
            return True
            
        except Exception as e:
            print(f"稳定性测试异常: {e}")
            return False
        finally:
            # 清理资源
            self.stop_all_processes()
    
    def print_test_results(self):
        """输出测试结果"""
        print("\n" + "=" * 60)
        print("稳定性测试结果")
        print("=" * 60)
        
        print(f"连接重置错误: {self.test_results['connection_resets']}")
        print(f"心跳超时错误: {self.test_results['heartbeat_timeouts']}")
        print(f"Gossip协议错误: {self.test_results['gossip_errors']}")
        print(f"EventBus错误: {self.test_results['eventbus_errors']}")
        print(f"成功连接数: {self.test_results['successful_connections']}")
        print(f"失败连接数: {self.test_results['failed_connections']}")
        print(f"总消息数: {self.test_results['total_messages']}")
        
        # 计算稳定性评分
        total_errors = (self.test_results['connection_resets'] + 
                       self.test_results['heartbeat_timeouts'] + 
                       self.test_results['gossip_errors'] + 
                       self.test_results['eventbus_errors'])
        
        success_rate = (self.test_results['successful_connections'] / 
                       max(1, self.test_results['successful_connections'] + self.test_results['failed_connections'])) * 100
        
        print(f"\n总错误数: {total_errors}")
        print(f"连接成功率: {success_rate:.1f}%")
        
        if total_errors <= 5 and success_rate >= 90:
            print("✅ 稳定性测试通过 - 系统稳定性良好")
        elif total_errors <= 15 and success_rate >= 80:
            print("⚠️  稳定性测试部分通过 - 系统稳定性一般")
        else:
            print("❌ 稳定性测试失败 - 系统稳定性差")
        
        # 保存测试结果
        result_file = f"stability_test_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(result_file, 'w', encoding='utf-8') as f:
            json.dump(self.test_results, f, indent=2, ensure_ascii=False)
        print(f"\n测试结果已保存到: {result_file}")

def main():
    """主函数"""
    if len(sys.argv) > 1 and sys.argv[1] == '--help':
        print("用法: python test_gossip_eventbus_stability.py")
        print("测试Gossip协议和EventBusServer的稳定性优化效果")
        return
    
    tester = StabilityTester()
    
    def signal_handler(signum, frame):
        print("\n收到中断信号，正在清理...")
        tester.stop_all_processes()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        success = tester.run_stability_test()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n测试被用户中断")
        tester.stop_all_processes()
        sys.exit(1)
    except Exception as e:
        print(f"测试执行异常: {e}")
        tester.stop_all_processes()
        sys.exit(1)

if __name__ == "__main__":
    main() 