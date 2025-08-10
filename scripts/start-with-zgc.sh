#!/bin/bash

# V-MQTT 高性能启动脚本 - ZGC优化版本
# 针对单机百万连接场景进行优化

set -e

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 配置参数
APP_NAME="vmqtt-core"
JAR_FILE="vmqtt-core/target/vmqtt-core-1.0.0-SNAPSHOT.jar"
JAVA_HOME=${JAVA_HOME:-$(dirname $(dirname $(readlink -f $(which java))))}
LOG_DIR="logs"
PID_FILE="$LOG_DIR/${APP_NAME}.pid"

# 检查Java版本
check_java_version() {
    echo -e "${BLUE}检查Java版本...${NC}"
    
    if [[ -z "$JAVA_HOME" ]]; then
        echo -e "${RED}错误: JAVA_HOME未设置${NC}"
        exit 1
    fi
    
    JAVA_VERSION=$("$JAVA_HOME/bin/java" -version 2>&1 | grep -oP 'version "([0-9]+)' | grep -oP '[0-9]+$')
    
    if [[ $JAVA_VERSION -lt 21 ]]; then
        echo -e "${RED}错误: 需要Java 21或更高版本，当前版本: $JAVA_VERSION${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}Java版本检查通过: $JAVA_VERSION${NC}"
}

# 检查系统资源
check_system_resources() {
    echo -e "${BLUE}检查系统资源...${NC}"
    
    # 检查内存
    TOTAL_MEM=$(free -m | awk 'NR==2{printf "%.0f", $2/1024}')
    if [[ $TOTAL_MEM -lt 16 ]]; then
        echo -e "${YELLOW}警告: 系统内存不足16GB，当前: ${TOTAL_MEM}GB${NC}"
        echo -e "${YELLOW}建议至少16GB内存以获得最佳性能${NC}"
    else
        echo -e "${GREEN}内存检查通过: ${TOTAL_MEM}GB${NC}"
    fi
    
    # 检查CPU核心数
    CPU_CORES=$(nproc)
    if [[ $CPU_CORES -lt 8 ]]; then
        echo -e "${YELLOW}警告: CPU核心数不足8个，当前: ${CPU_CORES}个${NC}"
        echo -e "${YELLOW}建议至少8个CPU核心以获得最佳性能${NC}"
    else
        echo -e "${GREEN}CPU检查通过: ${CPU_CORES}个核心${NC}"
    fi
}

# 检查是否支持大页面
check_hugepages() {
    echo -e "${BLUE}检查大页面支持...${NC}"
    
    if [[ -f /proc/sys/vm/nr_hugepages ]]; then
        HUGEPAGES=$(cat /proc/sys/vm/nr_hugepages)
        echo -e "${GREEN}大页面数量: $HUGEPAGES${NC}"
        
        if [[ $HUGEPAGES -eq 0 ]]; then
            echo -e "${YELLOW}建议启用大页面以提升ZGC性能${NC}"
            echo -e "${YELLOW}运行: sudo sysctl vm.nr_hugepages=1024${NC}"
        fi
    else
        echo -e "${YELLOW}警告: 无法检查大页面支持${NC}"
    fi
}

# 创建日志目录
create_log_directory() {
    if [[ ! -d "$LOG_DIR" ]]; then
        mkdir -p "$LOG_DIR"
        echo -e "${GREEN}创建日志目录: $LOG_DIR${NC}"
    fi
}

# ZGC优化的JVM参数
get_jvm_options() {
    local heap_size="8g"
    local direct_memory="4g"
    local zgc_heap_soft_max="7g"
    
    # 根据系统内存动态调整
    local total_mem_gb=$(free -m | awk 'NR==2{printf "%.0f", $2/1024}')
    if [[ $total_mem_gb -ge 32 ]]; then
        heap_size="16g"
        direct_memory="8g"
        zgc_heap_soft_max="15g"
    elif [[ $total_mem_gb -ge 16 ]]; then
        heap_size="12g"
        direct_memory="6g"
        zgc_heap_soft_max="11g"
    fi
    
    echo "
        # 内存配置
        -Xms${heap_size}
        -Xmx${heap_size}
        -XX:MaxDirectMemorySize=${direct_memory}
        
        # ZGC垃圾回收器配置
        -XX:+UseZGC
        -XX:+UnlockExperimentalVMOptions
        -XX:SoftMaxHeapSize=${zgc_heap_soft_max}
        -XX:+UseTransparentHugePages
        -XX:+UseLargePages
        
        # ZGC调优参数
        -XX:ZCollectionInterval=5000
        -XX:ZUncommitDelay=300
        -XX:ZCollectionBasedAllocation=true
        
        # 虚拟线程支持
        --enable-preview
        --add-modules jdk.incubator.concurrent
        
        # 性能优化
        -XX:+UseStringDeduplication
        -XX:+OptimizeStringConcat
        -XX:+UseCompressedOops
        -XX:+UseCompressedClassPointers
        -XX:+UseG1GC
        -XX:+DisableExplicitGC
        
        # 编译优化
        -XX:+TieredCompilation
        -XX:TieredStopAtLevel=4
        -XX:+UseCodeCacheFlushing
        -XX:ReservedCodeCacheSize=256m
        
        # I/O优化
        -Djava.nio.channels.spi.SelectorProvider=sun.nio.ch.EPollSelectorProvider
        -Dio.netty.noUnsafe=false
        -Dio.netty.tryReflectionSetAccessible=true
        -Dio.netty.allocator.type=pooled
        -Dio.netty.allocator.numDirectArenas=32
        -Dio.netty.allocator.numHeapArenas=32
        
        # 网络优化
        -Dsun.net.useExclusiveBind=true
        -Dsun.net.useExclusiveBind.udp=true
        
        # 日志配置
        -XX:+PrintGC
        -XX:+PrintGCDetails
        -XX:+PrintGCTimeStamps
        -XX:+PrintGCApplicationStoppedTime
        -XX:+PrintGCApplicationConcurrentTime
        -Xloggc:${LOG_DIR}/gc.log
        -XX:+UseGCLogFileRotation
        -XX:NumberOfGCLogFiles=5
        -XX:GCLogFileSize=100M
        
        # JFR性能分析
        -XX:+FlightRecorder
        -XX:StartFlightRecording=duration=60s,filename=${LOG_DIR}/startup.jfr
        
        # 调试信息
        -XX:+PrintCommandLineFlags
        -XX:+PrintVMOptions
        
        # 安全优化
        -Djava.security.egd=file:/dev/urandom
        
        # Spring Boot配置
        -Dspring.profiles.active=production
        -Dserver.tomcat.max-threads=200
        -Dserver.tomcat.min-spare-threads=20"
}

# 启动应用
start_application() {
    echo -e "${BLUE}启动V-MQTT服务器...${NC}"
    
    if [[ ! -f "$JAR_FILE" ]]; then
        echo -e "${RED}错误: JAR文件不存在: $JAR_FILE${NC}"
        echo -e "${YELLOW}请先运行: mvn clean package${NC}"
        exit 1
    fi
    
    # 检查是否已经运行
    if [[ -f "$PID_FILE" ]] && kill -0 $(cat "$PID_FILE") 2>/dev/null; then
        echo -e "${YELLOW}应用已在运行，PID: $(cat $PID_FILE)${NC}"
        exit 1
    fi
    
    local jvm_options=$(get_jvm_options)
    
    echo -e "${GREEN}使用JVM参数:${NC}"
    echo "$jvm_options" | grep -v '^[[:space:]]*$' | sed 's/^[[:space:]]*/  /'
    
    nohup "$JAVA_HOME/bin/java" $jvm_options -jar "$JAR_FILE" \
        > "$LOG_DIR/${APP_NAME}.out" 2>&1 &
    
    local pid=$!
    echo $pid > "$PID_FILE"
    
    echo -e "${GREEN}应用已启动，PID: $pid${NC}"
    echo -e "${GREEN}日志文件: ${LOG_DIR}/${APP_NAME}.out${NC}"
    echo -e "${GREEN}GC日志: ${LOG_DIR}/gc.log${NC}"
    
    # 等待应用启动
    echo -e "${BLUE}等待应用启动...${NC}"
    sleep 10
    
    # 检查应用是否成功启动
    if kill -0 $pid 2>/dev/null; then
        echo -e "${GREEN}✓ 应用启动成功！${NC}"
        echo -e "${GREEN}✓ 服务地址: http://localhost:8080${NC}"
        echo -e "${GREEN}✓ MQTT端口: 1883${NC}"
        echo -e "${GREEN}✓ MQTTS端口: 8883${NC}"
    else
        echo -e "${RED}✗ 应用启动失败${NC}"
        echo -e "${YELLOW}查看日志: tail -f ${LOG_DIR}/${APP_NAME}.out${NC}"
        exit 1
    fi
}

# 停止应用
stop_application() {
    echo -e "${BLUE}停止V-MQTT服务器...${NC}"
    
    if [[ ! -f "$PID_FILE" ]]; then
        echo -e "${YELLOW}PID文件不存在，应用可能未运行${NC}"
        return
    fi
    
    local pid=$(cat "$PID_FILE")
    
    if kill -0 $pid 2>/dev/null; then
        echo -e "${BLUE}正在停止应用，PID: $pid${NC}"
        kill $pid
        
        # 等待优雅停止
        local count=0
        while kill -0 $pid 2>/dev/null && [[ $count -lt 30 ]]; do
            sleep 1
            ((count++))
        done
        
        if kill -0 $pid 2>/dev/null; then
            echo -e "${YELLOW}强制停止应用${NC}"
            kill -9 $pid
        fi
        
        echo -e "${GREEN}应用已停止${NC}"
    else
        echo -e "${YELLOW}应用未运行${NC}"
    fi
    
    rm -f "$PID_FILE"
}

# 重启应用
restart_application() {
    stop_application
    sleep 2
    start_application
}

# 显示状态
show_status() {
    if [[ -f "$PID_FILE" ]] && kill -0 $(cat "$PID_FILE") 2>/dev/null; then
        local pid=$(cat "$PID_FILE")
        echo -e "${GREEN}应用运行中，PID: $pid${NC}"
        
        # 显示资源使用情况
        local cpu_usage=$(ps -p $pid -o %cpu --no-headers)
        local mem_usage=$(ps -p $pid -o %mem --no-headers)
        local mem_size=$(ps -p $pid -o rss --no-headers)
        
        echo -e "CPU使用率: ${cpu_usage}%"
        echo -e "内存使用率: ${mem_usage}%"
        echo -e "内存大小: $((mem_size/1024))MB"
    else
        echo -e "${RED}应用未运行${NC}"
    fi
}

# 显示日志
show_logs() {
    if [[ -f "$LOG_DIR/${APP_NAME}.out" ]]; then
        echo -e "${BLUE}显示应用日志（Ctrl+C退出）:${NC}"
        tail -f "$LOG_DIR/${APP_NAME}.out"
    else
        echo -e "${YELLOW}日志文件不存在${NC}"
    fi
}

# 主函数
main() {
    case "${1:-start}" in
        "start")
            create_log_directory
            check_java_version
            check_system_resources
            check_hugepages
            start_application
            ;;
        "stop")
            stop_application
            ;;
        "restart")
            restart_application
            ;;
        "status")
            show_status
            ;;
        "logs")
            show_logs
            ;;
        *)
            echo "用法: $0 {start|stop|restart|status|logs}"
            echo ""
            echo "命令说明:"
            echo "  start   - 启动V-MQTT服务器（默认）"
            echo "  stop    - 停止V-MQTT服务器"
            echo "  restart - 重启V-MQTT服务器"
            echo "  status  - 显示运行状态"
            echo "  logs    - 实时显示日志"
            exit 1
            ;;
    esac
}

# 脚本入口
main "$@"