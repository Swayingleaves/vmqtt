#!/usr/bin/env python3
"""
简化的稳定性测试脚本
专注于验证Gossip协议和EventBusServer的基本稳定性

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
from datetime import datetime

class SimpleStabilityTester:
    def __init__(self):
        self.fe_processes = []
        self.be_processes = []
        
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
        
        time.sleep(3)
    
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
            time.sleep(5)  # 等待启动
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
            time.sleep(5)  # 等待启动
            return process
        except Exception as e:
            print(f"启动BE节点失败: {e}")
            return None
    
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
                return True
            else:
                print(f"BE节点注册失败: {response.status_code}")
                return False
        except Exception as e:
            print(f"BE节点注册异常: {e}")
            return False
    
    def check_fe_health(self, fe_port):
        """检查FE节点健康状态"""
        try:
            response = requests.get(f"http://localhost:{fe_port}/actuator/health", timeout=5)
            if response.status_code == 200:
                health_data = response.json()
                return health_data.get('status') == 'UP'
            return False
        except Exception as e:
            print(f"检查FE健康状态失败 (端口{fe_port}): {e}")
            return False
    
    def check_gossip_cluster(self, fe_port):
        """检查Gossip集群状态"""
        try:
            response = requests.get(f"http://localhost:{fe_port}/api/fe/cluster", timeout=5)
            if response.status_code == 200:
                cluster_data = response.json()
                return cluster_data
            return None
        except Exception as e:
            print(f"检查Gossip集群状态失败 (端口{fe_port}): {e}")
            return None
    
    def count_log_errors(self, log_file, error_patterns):
        """统计日志文件中的错误"""
        if not os.path.exists(log_file):
            return {}
        
        error_counts = {}
        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                content = f.read()
                
            for pattern in error_patterns:
                count = content.count(pattern)
                error_counts[pattern] = count
                
        except Exception as e:
            print(f"读取日志文件失败 {log_file}: {e}")
            
        return error_counts
    
    def run_basic_test(self):
        """运行基本稳定性测试"""
        print("=" * 60)
        print("开始基本稳定性测试")
        print("=" * 60)
        
        try:
            # 1. 停止所有现有进程
            self.stop_all_processes()
            
            # 2. 启动FE1节点
            print("\n1. 启动FE1节点...")
            fe1 = self.start_fe_node('fe1-config.yml')
            if not fe1:
                print("FE1节点启动失败")
                return False
            
            # 3. 检查FE1健康状态
            print("\n2. 检查FE1健康状态...")
            time.sleep(3)
            if not self.check_fe_health(8081):
                print("FE1节点不健康")
                return False
            print("✅ FE1节点健康")
            
            # 4. 启动FE2节点
            print("\n3. 启动FE2节点...")
            fe2 = self.start_fe_node('fe2-config.yml')
            if not fe2:
                print("FE2节点启动失败")
                return False
            
            # 5. 检查FE2健康状态
            print("\n4. 检查FE2健康状态...")
            time.sleep(3)
            if not self.check_fe_health(8082):
                print("FE2节点不健康")
                return False
            print("✅ FE2节点健康")
            
            # 6. 检查Gossip集群发现
            print("\n5. 检查Gossip集群发现...")
            time.sleep(10)  # 等待Gossip协议同步
            
            cluster1 = self.check_gossip_cluster(8081)
            cluster2 = self.check_gossip_cluster(8082)
            
            if cluster1 and cluster2:
                nodes1 = cluster1.get('clusterNodes', [])
                nodes2 = cluster2.get('clusterNodes', [])
                
                if len(nodes1) >= 2 and len(nodes2) >= 2:
                    print("✅ Gossip集群发现成功")
                    print(f"   FE1发现节点数: {len(nodes1)}")
                    print(f"   FE2发现节点数: {len(nodes2)}")
                else:
                    print("⚠️  Gossip集群发现部分成功")
                    print(f"   FE1发现节点数: {len(nodes1)}")
                    print(f"   FE2发现节点数: {len(nodes2)}")
            else:
                print("❌ Gossip集群发现失败")
                return False
            
            # 7. 启动BE节点
            print("\n6. 启动BE节点...")
            be1 = self.start_be_node('be1-config.yml')
            if not be1:
                print("BE节点启动失败")
                return False
            
            # 8. 注册BE到FE
            print("\n7. 注册BE节点到FE...")
            time.sleep(3)
            if not self.register_be_to_fe():
                print("BE节点注册失败")
                return False
            
            # 9. 等待系统稳定并检查错误
            print("\n8. 等待系统稳定并检查错误...")
            time.sleep(15)
            
            # 检查日志错误
            error_patterns = [
                'java.net.SocketException: Connection reset',
                '节点疑似故障',
                'BindException: Address already in use',
                'EventBus连接失败'
            ]
            
            log_files = [
                '../output/logs/fe1/fe-server.log',
                '../output/logs/fe2/fe-server.log',
                '../output/logs/be/be-server.log'
            ]
            
            total_errors = 0
            for log_file in log_files:
                error_counts = self.count_log_errors(log_file, error_patterns)
                for pattern, count in error_counts.items():
                    if count > 0:
                        print(f"⚠️  发现错误 '{pattern}': {count} 次 (文件: {log_file})")
                        total_errors += count
            
            # 10. 输出测试结果
            print("\n" + "=" * 60)
            print("基本稳定性测试结果")
            print("=" * 60)
            
            if total_errors == 0:
                print("✅ 测试通过 - 系统稳定性良好，无错误发现")
                return True
            elif total_errors <= 5:
                print(f"⚠️  测试部分通过 - 发现 {total_errors} 个错误，但在可接受范围内")
                return True
            else:
                print(f"❌ 测试失败 - 发现 {total_errors} 个错误，系统稳定性差")
                return False
            
        except Exception as e:
            print(f"测试执行异常: {e}")
            return False
        finally:
            # 清理资源
            print("\n9. 清理测试环境...")
            self.stop_all_processes()

def main():
    """主函数"""
    if len(sys.argv) > 1 and sys.argv[1] == '--help':
        print("用法: python test_simple_stability.py")
        print("运行基本的稳定性测试")
        return
    
    tester = SimpleStabilityTester()
    
    def signal_handler(signum, frame):
        print("\n收到中断信号，正在清理...")
        tester.stop_all_processes()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        success = tester.run_basic_test()
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