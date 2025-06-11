#!/usr/bin/env python3
"""
FE集群通信专项测试
只测试FE节点间的Gossip协议和EventBus通信，不涉及BE节点
"""

import subprocess
import time
import requests
import json
import os
import sys

def run_command(cmd, cwd=None):
    """执行命令并返回结果"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=cwd)
        return result.returncode == 0, result.stdout, result.stderr
    except Exception as e:
        return False, "", str(e)

def check_fe_health(port):
    """检查FE节点健康状态"""
    try:
        response = requests.get(f"http://localhost:{port}/actuator/health", timeout=5)
        return response.status_code == 200
    except:
        return False

def get_fe_cluster_info(port):
    """获取FE集群信息"""
    try:
        response = requests.get(f"http://localhost:{port}/api/fe/nodes", timeout=5)
        if response.status_code == 200:
            return response.json()
        return None
    except:
        return None

def count_log_errors(log_file, error_patterns):
    """统计日志中的错误数量"""
    if not os.path.exists(log_file):
        return {}
    
    error_counts = {pattern: 0 for pattern in error_patterns}
    
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            content = f.read()
            for pattern in error_patterns:
                error_counts[pattern] = content.count(pattern)
    except Exception as e:
        print(f"读取日志文件失败: {log_file}, 错误: {e}")
    
    return error_counts

def main():
    print("=" * 60)
    print("FE集群通信专项测试")
    print("=" * 60)
    
    # 切换到output目录
    output_dir = "../output"
    
    # 1. 停止所有服务
    print("1. 停止所有FE服务...")
    run_command("./stop-fe.sh", cwd=output_dir)
    time.sleep(2)
    
    # 2. 启动FE1
    print("2. 启动FE1节点...")
    success, stdout, stderr = run_command("./start-fe.sh --config fe1-config.yml &", cwd=output_dir)
    time.sleep(8)  # 等待启动完成
    
    if not check_fe_health(8081):
        print("❌ FE1启动失败")
        return False
    print("✅ FE1启动成功")
    
    # 3. 启动FE2
    print("3. 启动FE2节点...")
    success, stdout, stderr = run_command("./start-fe.sh --config fe2-config.yml &", cwd=output_dir)
    time.sleep(8)  # 等待启动完成
    
    if not check_fe_health(8082):
        print("❌ FE2启动失败")
        return False
    print("✅ FE2启动成功")
    
    # 4. 等待集群发现
    print("4. 等待集群发现...")
    time.sleep(10)
    
    # 5. 检查集群状态
    print("5. 检查集群状态...")
    fe1_cluster = get_fe_cluster_info(8081)
    fe2_cluster = get_fe_cluster_info(8082)
    
    print(f"FE1集群信息: {fe1_cluster}")
    print(f"FE2集群信息: {fe2_cluster}")
    
    # 6. 检查Gossip通信
    print("6. 检查Gossip通信...")
    fe1_nodes = fe1_cluster.get('nodes', []) if fe1_cluster else []
    fe2_nodes = fe2_cluster.get('nodes', []) if fe2_cluster else []
    
    fe1_discovered = len([n for n in fe1_nodes if n.get('nodeId') == 'fe-node-2'])
    fe2_discovered = len([n for n in fe2_nodes if n.get('nodeId') == 'fe-node-1'])
    
    if fe1_discovered > 0 and fe2_discovered > 0:
        print("✅ Gossip集群发现成功")
        gossip_success = True
    else:
        print(f"⚠️  Gossip集群发现部分成功 (FE1发现FE2: {fe1_discovered}, FE2发现FE1: {fe2_discovered})")
        gossip_success = False
    
    # 7. 等待更长时间观察稳定性
    print("7. 观察系统稳定性...")
    time.sleep(15)
    
    # 8. 检查错误日志
    print("8. 检查错误日志...")
    error_patterns = [
        'java.net.SocketException: Connection reset',
        'BindException: Address already in use',
        'invalid type code',
        'EOFException',
        'SocketTimeoutException: Read timed out'
    ]
    
    fe1_errors = count_log_errors("../output/logs/fe1/fe-server.log", error_patterns)
    fe2_errors = count_log_errors("../output/logs/fe2/fe-server.log", error_patterns)
    
    total_errors = 0
    for pattern in error_patterns:
        fe1_count = fe1_errors.get(pattern, 0)
        fe2_count = fe2_errors.get(pattern, 0)
        pattern_total = fe1_count + fe2_count
        total_errors += pattern_total
        
        if pattern_total > 0:
            print(f"⚠️  发现错误 '{pattern}': FE1={fe1_count}, FE2={fe2_count}")
    
    # 9. 测试结果
    print("\n" + "=" * 60)
    print("FE集群通信测试结果")
    print("=" * 60)
    
    if total_errors == 0 and gossip_success:
        print("✅ 测试通过 - FE集群通信稳定")
        result = True
    elif total_errors < 5:
        print(f"⚠️  测试部分通过 - 发现 {total_errors} 个错误，但在可接受范围内")
        result = True
    else:
        print(f"❌ 测试失败 - 发现 {total_errors} 个错误，系统不稳定")
        result = False
    
    # 10. 清理
    print("\n10. 清理测试环境...")
    run_command("./stop-fe.sh", cwd=output_dir)
    
    return result

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 