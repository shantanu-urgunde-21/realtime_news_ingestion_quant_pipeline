import os
import sys

try:
    import clickhouse_connect
except ImportError:
    print("❌ Error: clickhouse-connect is required. Install it using: pip install clickhouse-connect")
    sys.exit(1)

try:
    import pandas as pd
except ImportError:
    print("❌ Error: pandas is required. Install it using: pip install pandas")
    sys.exit(1)

try:
    import matplotlib.pyplot as plt
except ImportError:
    print("❌ Error: matplotlib is required to plot. Install it using: pip install matplotlib")
    sys.exit(1)

def plot_latencies(client):
    print("📊 Fetching latency metrics from ClickHouse...")
    query = """
    SELECT 
        timestamp,
        service_name,
        metric_name,
        latency_ms
    FROM telemetry.pipeline_latencies
    WHERE timestamp >= now() - INTERVAL 2 HOUR
    ORDER BY timestamp ASC
    """
    try:
        df = client.query_df(query)
    except Exception as e:
        print(f"❌ Query failed: {e}")
        return

    if df.empty:
        print("⚠️ No latency metrics found in the last 2 hours. Start your pipeline to generate load!")
        return

    print(f"📈 Plotting latency metrics ({len(df)} samples)...")
    plt.figure(figsize=(12, 6))
    
    # Group by service & metric and plot each time series
    for name, group in df.groupby(['service_name', 'metric_name']):
        plt.plot(group['timestamp'], group['latency_ms'], label=f"{name[0]} - {name[1]}", alpha=0.8)
        
    plt.title("Quantitative Pipeline Processing Latencies (Last 2 Hours)", fontsize=13, fontweight='bold')
    plt.xlabel("Timestamp", fontsize=11)
    plt.ylabel("Latency (ms)", fontsize=11)
    plt.legend(loc="upper right", frameon=True, shadow=True)
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.tight_layout()
    
    output_path = "telemetry_latencies.png"
    plt.savefig(output_path, dpi=300)
    print(f"✓ Saved latency graph to: {output_path}")

def plot_resources(client):
    print("📊 Fetching system utilization metrics from ClickHouse...")
    query = """
    SELECT 
        timestamp,
        service_name,
        cpu_utilization_pct,
        memory_used_mb
    FROM telemetry.system_metrics
    WHERE timestamp >= now() - INTERVAL 2 HOUR
    ORDER BY timestamp ASC
    """
    try:
        df = client.query_df(query)
    except Exception as e:
        print(f"❌ Query failed: {e}")
        return

    if df.empty:
        print("⚠️ No system metrics found in the last 2 hours. Start your pipeline to generate load!")
        return

    print(f"📈 Plotting system resource metrics ({len(df)} samples)...")
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 9), sharex=True)

    # Plot CPU
    for name, group in df.groupby('service_name'):
        ax1.plot(group['timestamp'], group['cpu_utilization_pct'], label=name, alpha=0.8)
    ax1.set_title("Container CPU Utilization (%)", fontsize=11, fontweight='bold')
    ax1.set_ylabel("CPU (%)", fontsize=10)
    ax1.grid(True, linestyle="--", alpha=0.5)
    ax1.legend(loc="upper right")

    # Plot Memory
    for name, group in df.groupby('service_name'):
        ax2.plot(group['timestamp'], group['memory_used_mb'], label=name, alpha=0.8)
    ax2.set_title("Container Memory Utilization (MB)", fontsize=11, fontweight='bold')
    ax2.set_ylabel("Memory (MB)", fontsize=10)
    ax2.set_xlabel("Timestamp", fontsize=10)
    ax2.grid(True, linestyle="--", alpha=0.5)
    ax2.legend(loc="upper right")

    plt.suptitle("Quantitative Microservices System Resources (Last 2 Hours)", fontsize=13, fontweight='bold')
    plt.tight_layout()
    
    output_path = "telemetry_resources.png"
    plt.savefig(output_path, dpi=300)
    print(f"✓ Saved system resources graph to: {output_path}")

def main():
    host = os.getenv("CLICKHOUSE_HOST", "127.0.0.1")
    port = int(os.getenv("CLICKHOUSE_PORT", "8124"))
    user = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "password")

    print(f"🔌 Connecting to ClickHouse Monitoring at {host}:{port}...")
    try:
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password
        )
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("Please verify the 'clickhouse_monitoring' container is running on port 8124.")
        sys.exit(1)

    plot_latencies(client)
    print("-" * 60)
    plot_resources(client)

if __name__ == "__main__":
    main()
