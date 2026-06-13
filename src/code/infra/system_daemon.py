import os
import time
import threading
import logging
import psutil
from infra.telemetry_client import TelemetryClient

logger = logging.getLogger("system_daemon")

class SystemTelemetryDaemon:
    """
    Background daemon thread that periodically collects CPU, Memory, Disk, and
    Network utilization metrics for the container/process and logs them to ClickHouse.
    """
    def __init__(self, service_name, interval_seconds=10):
        self.service_name = service_name
        self.interval = interval_seconds
        self.telemetry = TelemetryClient()
        self.stop_event = threading.Event()
        self.thread = None

    def start(self):
        """Start the background metrics collection thread."""
        if self.thread is not None and self.thread.is_alive():
            logger.warning(f"SystemTelemetryDaemon for {self.service_name} is already running.")
            return
        
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._run, name=f"sys-daemon-{self.service_name}", daemon=True)
        self.thread.start()
        logger.info(f"SystemTelemetryDaemon started for service '{self.service_name}' with interval {self.interval}s")

    def stop(self):
        """Stop the background metrics collection thread."""
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=3.0)
            logger.info(f"SystemTelemetryDaemon stopped for service '{self.service_name}'")

    def _run(self):
        # Initialize net IO counters to measure delta bytes
        try:
            last_net_io = psutil.net_io_counters()
            last_time = time.time()
        except Exception:
            last_net_io = None
            last_time = time.time()

        # To get realistic CPU utilization, perform an initial query
        try:
            psutil.cpu_percent(interval=None)
        except Exception:
            pass

        while not self.stop_event.is_set():
            # Wait for the interval or stop event
            if self.stop_event.wait(self.interval):
                break
            
            try:
                # 1. CPU utilization percent
                cpu_util = float(psutil.cpu_percent(interval=None))

                # 2. Memory metrics (MB)
                vm = psutil.virtual_memory()
                mem_used = float(vm.used / (1024 * 1024))
                mem_total = float(vm.total / (1024 * 1024))

                # If running inside Docker on Linux, we can try reading cgroups to get actual container memory
                # limit and usage instead of the host machine's statistics.
                cgroup_mem_limit_path = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
                cgroup_mem_usage_path = "/sys/fs/cgroup/memory/memory.usage_in_bytes"
                # For cgroups v2
                cgroup_v2_mem_limit_path = "/sys/fs/cgroup/memory.max"
                cgroup_v2_mem_usage_path = "/sys/fs/cgroup/memory.current"

                if os.path.exists(cgroup_mem_limit_path) and os.path.exists(cgroup_mem_usage_path):
                    try:
                        with open(cgroup_mem_limit_path, "r") as f:
                            limit = int(f.read().strip())
                        with open(cgroup_mem_usage_path, "r") as f:
                            usage = int(f.read().strip())
                        # If memory limit is a huge number (meaning no limit), default to virtual memory total
                        if limit < 9e15:
                            mem_total = float(limit / (1024 * 1024))
                            mem_used = float(usage / (1024 * 1024))
                    except Exception:
                        pass
                elif os.path.exists(cgroup_v2_mem_limit_path) and os.path.exists(cgroup_v2_mem_usage_path):
                    try:
                        with open(cgroup_v2_mem_limit_path, "r") as f:
                            limit_str = f.read().strip()
                            limit = int(limit_str) if limit_str != "max" else 9e18
                        with open(cgroup_v2_mem_usage_path, "r") as f:
                            usage = int(f.read().strip())
                        if limit < 9e15:
                            mem_total = float(limit / (1024 * 1024))
                            mem_used = float(usage / (1024 * 1024))
                    except Exception:
                        pass

                # 3. Disk usage (GB)
                try:
                    disk = psutil.disk_usage('/')
                    disk_used = float(disk.used / (1024 * 1024 * 1024))
                except Exception:
                    disk_used = 0.0

                # 4. Network RX/TX Bytes
                rx_bytes = 0
                tx_bytes = 0
                try:
                    current_net_io = psutil.net_io_counters()
                    if last_net_io and current_net_io:
                        rx_bytes = int(current_net_io.bytes_recv - last_net_io.bytes_recv)
                        tx_bytes = int(current_net_io.bytes_sent - last_net_io.bytes_sent)
                    last_net_io = current_net_io
                except Exception:
                    pass

                # Log metrics asynchronously
                self.telemetry.log_system_metrics(
                    service_name=self.service_name,
                    cpu_utilization_pct=cpu_util,
                    memory_used_mb=mem_used,
                    memory_total_mb=mem_total,
                    disk_used_gb=disk_used,
                    network_rx_bytes=rx_bytes,
                    network_tx_bytes=tx_bytes
                )
                
            except Exception as e:
                # Catch any unexpected errors so daemon never crashes the main process
                print(f"[SystemTelemetryDaemon Error] Failed to harvest system metrics: {e}")


_daemon_lock = threading.Lock()
_active_daemon = None

def start_system_daemon(service_name, interval_seconds=10):
    """
    Convenience function to start a singleton system daemon for the service.
    """
    global _active_daemon
    with _daemon_lock:
        if _active_daemon is None:
            _active_daemon = SystemTelemetryDaemon(service_name, interval_seconds)
            _active_daemon.start()
        return _active_daemon
