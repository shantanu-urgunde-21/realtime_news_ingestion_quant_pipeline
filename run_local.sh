#!/usr/bin/env bash

# ==============================================================================
# Sentinel-Stream: Quantitative Pipeline Local Orchestrator
# Automates the startup, stopping, status monitoring, and log tailing of the
# microservices running locally in hybrid development mode.
# ==============================================================================

set -eo pipefail

# Directory configurations
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="${SCRIPT_DIR}/src/code/logs"
VENV_DIR="${SCRIPT_DIR}/venv"
PYTHON_EXEC="${VENV_DIR}/bin/python3"

# Set up PYTHONPATH so infra packages are correctly imported
export PYTHONPATH="${SCRIPT_DIR}/src/code"

# Hostname mapping check
check_hosts() {
    echo -n "🔍 Verifying /etc/hosts mapping..."
    if grep -q "clickhouse_monitoring" /etc/hosts && grep -q "kafka" /etc/hosts; then
        echo -e " \033[0;32m[OK]\033[0m"
    else
        echo -e " \033[0;31m[WARNING]\033[0m"
        echo -e "⚠️  Please run the following command to add hosts mappings for external access:"
        echo -e "   \033[1msudo echo \"127.0.0.1 kafka clickhouse clickhouse_monitoring\" | sudo tee -a /etc/hosts\033[0m"
    fi
}

start_infra() {
    echo "🐳 Starting backing databases and Kafka brokers..."
    docker compose up -d kafka clickhouse clickhouse_monitoring
    
    echo "⏳ Waiting for ClickHouse & Kafka to pass health checks..."
    while true; do
        KAFKA_STATUS=$(docker inspect --format='{{json .State.Health.Status}}' kafka 2>/dev/null || echo "\"starting\"")
        CH_STATUS=$(docker inspect --format='{{json .State.Health.Status}}' clickhouse 2>/dev/null || echo "\"starting\"")
        CHM_STATUS=$(docker inspect --format='{{json .State.Health.Status}}' clickhouse_monitoring 2>/dev/null || echo "\"starting\"")
        
        if [ "$KAFKA_STATUS" == "\"healthy\"" ] && [ "$CH_STATUS" == "\"healthy\"" ] && [ "$CHM_STATUS" == "\"healthy\"" ]; then
            echo -e "🚀 \033[0;32mAll database and messaging services are HEALTHY!\033[0m"
            break
        else
            echo "   ... still starting (Kafka: $KAFKA_STATUS, ClickHouse: $CH_STATUS, ClickHouse Monitoring: $CHM_STATUS). Waiting 5s..."
            sleep 5
        fi
    done
}

start_services() {
    mkdir -p "$LOG_DIR"
    
    echo "📋 Starting local microservices..."

    # 1. Backend Service
    echo -n "   -> Launching Backend Service..."
    (cd "${SCRIPT_DIR}/src/code/backend" && KAFKA_BROKER=kafka:9092 USE_DUMMY_FIREBASE=true "$PYTHON_EXEC" main.py > "${LOG_DIR}/backend.log" 2>&1) &
    echo -e " \033[0;32m[RUNNING]\033[0m (Log: src/code/logs/backend.log)"
    sleep 2

    # 2. Decision Service
    echo -n "   -> Launching Decision Service (XGBoost ML)..."
    (cd "${SCRIPT_DIR}/src/code/decision_service" && KAFKA_BROKER=kafka:9092 CLICKHOUSE_HOST=clickhouse CLICKHOUSE_PORT=9000 "$PYTHON_EXEC" xgboost_mdl_inf.py > "${LOG_DIR}/decision_service.log" 2>&1) &
    echo -e " \033[0;32m[RUNNING]\033[0m (Log: src/code/logs/decision_service.log)"
    sleep 2

    # 3. Calculation Service
    echo -n "   -> Launching Calculation Service (Pathway Stream)..."
    (cd "${SCRIPT_DIR}/src/code/calc_service" && KAFKA_BROKER=kafka:9092 "$PYTHON_EXEC" main.py > "${LOG_DIR}/calc_service.log" 2>&1) &
    echo -e " \033[0;32m[RUNNING]\033[0m (Log: src/code/logs/calc_service.log)"
    sleep 3

    # 4. News Service
    echo -n "   -> Launching News Service (Headline Parser)..."
    (cd "${SCRIPT_DIR}/src/code/news_service" && KAFKA_BROKER=kafka:9092 USE_DUMMY_API=true "$PYTHON_EXEC" main.py > "${LOG_DIR}/news_service.log" 2>&1) &
    echo -e " \033[0;32m[RUNNING]\033[0m (Log: src/code/logs/news_service.log)"
    sleep 2

    # 5. Stock Service
    echo -n "   -> Launching Stock Service (Trigger Feed)..."
    (cd "${SCRIPT_DIR}/src/code/stock_service" && KAFKA_BROKER=kafka:9092 REPLAY_SPEEDUP=5.0 "$PYTHON_EXEC" main.py > "${LOG_DIR}/stock_service.log" 2>&1) &
    echo -e " \033[0;32m[RUNNING]\033[0m (Log: src/code/logs/stock_service.log)"
    sleep 2

    # 6. Kafka Monitor Daemon
    echo -n "   -> Launching Kafka Consumer Lag Monitor..."
    (cd "${SCRIPT_DIR}/src/code/infra" && KAFKA_BROKER=kafka:9092 "$PYTHON_EXEC" kafka_monitor.py > "${LOG_DIR}/kafka_monitor.log" 2>&1) &
    echo -e " \033[0;32m[RUNNING]\033[0m (Log: src/code/logs/kafka_monitor.log)"

    echo -e "\n🎉 \033[0;32mEntire Quantitative News Ingestion Pipeline started successfully!\033[0m"
}

stop_services() {
    echo "🛑 Shutting down all local Python microservices..."
    # Gracefully terminate python3 instances that execute src/code modules
    pkill -f "python3 src/code" || true
    pkill -f "main.py" || true
    pkill -f "xgboost_mdl_inf.py" || true
    pkill -f "kafka_monitor.py" || true
    echo -e " \033[0;32m[Microservices Terminated]\033[0m"
    
    echo "🐳 Stopping Docker containers..."
    docker compose stop
    echo -e " \033[0;32m[Docker Services Stopped]\033[0m"
}

print_status() {
    echo -e "\033[1m=== Pipeline Status ===\033[0m"
    # Query running python processes matching the src/code signature
    pgrep -fl "python3 src/code" || echo "No python microservices are running."
    
    echo -e "\n\033[1m=== Docker Infrastructure Status ===\033[0m"
    docker compose ps
}

tail_logs() {
    local target="$1"
    if [ -z "$target" ]; then
        echo "Usage: $0 logs [backend|decision_service|calc_service|news_service|stock_service|kafka_monitor]"
        exit 1
    fi
    local log_file="${LOG_DIR}/${target}.log"
    if [ -f "$log_file" ]; then
        echo -e "\033[1m--- Tailing logs for ${target} (${log_file}) ---\033[0m"
        tail -f -n 50 "$log_file"
    else
        echo "❌ Log file not found: $log_file"
        exit 1
    fi
}

query_telemetry() {
    local target="$1"
    local container_name="clickhouse_monitoring"
    local db_name="telemetry"
    local user="default"
    local password="password"

    if ! docker ps --filter "name=${container_name}" --filter "status=running" | grep -q "${container_name}"; then
        echo -e "\033[0;31m❌ Error: Container '${container_name}' is not running!\033[0m"
        echo "Please start the stack with './run_local.sh start' first."
        exit 1
    fi

    local run_query_cmd="docker exec -t ${container_name} clickhouse-client --user ${user} --password ${password} --database ${db_name} --query"

    case "$target" in
        latencies)
            echo -e "\033[1;34m📊 Querying Latency Percentiles (Last 1 Hour)...\033[0m"
            $run_query_cmd "SELECT service_name, metric_name, count() as samples, round(avg(latency_ms), 2) as avg_ms, round(quantile(0.50)(latency_ms), 2) as p50_ms, round(quantile(0.90)(latency_ms), 2) as p90_ms, round(quantile(0.99)(latency_ms), 2) as p99_ms FROM pipeline_latencies WHERE timestamp >= now() - INTERVAL 1 HOUR GROUP BY service_name, metric_name ORDER BY p99_ms DESC;"
            ;;
        lag)
            echo -e "\033[1;34m📊 Querying Kafka Active Lags and Processing Velocity...\033[0m"
            $run_query_cmd "SELECT topic_name, consumer_group, sum(consumer_lag) as total_lag, round(avg(messages_per_sec), 2) as avg_rate_msg_sec, max(latest_offset) as max_offset FROM kafka_metrics WHERE timestamp >= now() - INTERVAL 30 MINUTE GROUP BY topic_name, consumer_group ORDER BY total_lag DESC;"
            ;;
        resources)
            echo -e "\033[1;34m📊 Querying Peak & Average Container Resource Usage...\033[0m"
            $run_query_cmd "SELECT service_name, round(avg(cpu_utilization_pct), 1) as avg_cpu_pct, round(max(cpu_utilization_pct), 1) as peak_cpu_pct, round(avg(memory_used_mb), 0) as avg_mem_mb, round(max(memory_used_mb), 0) as peak_mem_mb, round(max(memory_total_mb), 0) as limit_mem_mb FROM system_metrics WHERE timestamp >= now() - INTERVAL 12 HOUR GROUP BY service_name ORDER BY peak_cpu_pct DESC;"
            ;;
        errors)
            echo -e "\033[1;31m📊 Querying Error & Warning Counts (Last 24 Hours)...\033[0m"
            $run_query_cmd "SELECT service_name, log_level, count() as total_errors FROM service_logs WHERE log_level IN ('WARNING', 'ERROR', 'FATAL') AND timestamp >= now() - INTERVAL 24 HOUR GROUP BY service_name, log_level ORDER BY total_errors DESC;"
            ;;
        interactive)
            echo -e "\033[1;32m🐚 Opening interactive ClickHouse SQL shell...\033[0m"
            docker exec -it "${container_name}" clickhouse-client --user "${user}" --password "${password}" --database "${db_name}"
            ;;
        *)
            echo -e "\033[1mUsage:\033[0m $0 telemetry {latencies|lag|resources|errors|interactive}"
            exit 1
            ;;
    esac
}

case "$1" in
    start)
        check_hosts
        start_infra
        start_services
        ;;
    stop)
        stop_services
        ;;
    status)
        print_status
        ;;
    logs)
        tail_logs "$2"
        ;;
    telemetry)
        query_telemetry "$2"
        ;;
    *)
        echo "Usage: $0 {start|stop|status|logs [service_name]|telemetry [query_type]}"
        echo "   Logs services: backend, decision_service, calc_service, news_service, stock_service, kafka_monitor"
        echo "   Telemetry types: latencies, lag, resources, errors, interactive"
        exit 1
        ;;
esac
