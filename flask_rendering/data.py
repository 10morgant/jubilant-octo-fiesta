import time
import random
import math
from datetime import datetime, timezone
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# --- Config (match your server settings) ---
INFLUX_URL    = "http://localhost:8086"
INFLUX_TOKEN  = "vm-perf-admin-token"
INFLUX_ORG    = "vm_perf"
INFLUX_BUCKET = "vm_perf"

WRITE_INTERVAL = 5  # seconds between writes

# --- Simulated machines ---
SOURCES = [
    {"host": "jumpbox-01", "target": "vm-app-01"},
    {"host": "jumpbox-01", "target": "vm-db-01"},
    {"host": "jumpbox-02", "target": "vm-app-01"},
]

CPU_HOSTS = ["vm-app-01", "vm-db-01", "vm-web-01"]

# Each CPU metric: field prefix, baseline %, drift amplitude, jitter std-dev,
# spike value, spike probability (per sample)
CPU_METRIC_SPECS = [
    {"metric": "usage",     "base": 40.0, "drift_amp": 10.0, "jitter_std": 5.0,  "spike_val": 85.0, "spike_prob": 0.03},
    {"metric": "readiness", "base": 5.0,  "drift_amp": 2.0,  "jitter_std": 1.0,  "spike_val": 20.0, "spike_prob": 0.05},
    {"metric": "wait",      "base": 8.0,  "drift_amp": 3.0,  "jitter_std": 2.0,  "spike_val": 30.0, "spike_prob": 0.02},
]

client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

def simulate_latency(base_ms=12.0, t=0):
    drift    = math.sin(t / 60) * 3
    jitter   = random.gauss(0, 1.5)
    spike    = 40 if random.random() < 0.03 else 0
    raw = max(0.5, base_ms + drift + jitter + spike)
    return round(raw, 3)

def compute_percentiles(samples):
    s = sorted(samples)
    n = len(s)
    return {
        "min":  s[0],
        "p50":  s[int(n * 0.50)],
        "p95":  s[int(n * 0.95)],
        "p99":  s[int(n * 0.99)],
        "max":  s[-1],
        "avg":  round(sum(s) / n, 3),
    }

def simulate_cpu_samples(base, drift_amp, jitter_std, spike_val, spike_prob, t, n=20):
    samples = []
    for _ in range(n):
        drift  = math.sin(t / 90) * drift_amp
        jitter = random.gauss(0, jitter_std)
        spike  = spike_val if random.random() < spike_prob else 0
        val    = max(0.0, min(100.0, base + drift + jitter + spike))
        samples.append(round(val, 2))
    s = sorted(samples)
    return {"min": s[0], "avg": round(sum(s) / len(s), 2), "max": s[-1]}

t = 0
print(f"Writing to InfluxDB every {WRITE_INTERVAL}s — Ctrl+C to stop\n")

try:
    while True:
        t += WRITE_INTERVAL
        timestamp = datetime.now(timezone.utc)

        # --- Latency ---
        for src in SOURCES:
            base = random.uniform(8, 20)
            samples = [simulate_latency(base, t) for _ in range(20)]
            stats = compute_percentiles(samples)

            point = (
                Point("latency")
                .tag("host",   src["host"])
                .tag("target", src["target"])
                .field("min",  stats["min"])
                .field("p50",  stats["p50"])
                .field("p95",  stats["p95"])
                .field("p99",  stats["p99"])
                .field("max",  stats["max"])
                .field("avg",  stats["avg"])
                .time(timestamp, WritePrecision.S)
            )
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
            print(
                f"[{timestamp.strftime('%H:%M:%S')}] latency "
                f"{src['host']} → {src['target']}  "
                f"p50={stats['p50']}ms  p95={stats['p95']}ms  p99={stats['p99']}ms"
                + ("  ⚡ SPIKE" if stats["max"] > 40 else "")
            )

        # --- CPU ---
        for host in CPU_HOSTS:
            pt = Point("cpu").tag("host", host).time(timestamp, WritePrecision.S)
            for spec in CPU_METRIC_SPECS:
                stats = simulate_cpu_samples(
                    base=spec["base"],
                    drift_amp=spec["drift_amp"],
                    jitter_std=spec["jitter_std"],
                    spike_val=spec["spike_val"],
                    spike_prob=spec["spike_prob"],
                    t=t,
                )
                m = spec["metric"]
                pt.field(f"{m}_avg", stats["avg"])
                pt.field(f"{m}_min", stats["min"])
                pt.field(f"{m}_max", stats["max"])
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=pt)
            print(
                f"[{timestamp.strftime('%H:%M:%S')}] cpu     {host}  "
                + "  ".join(
                    f"{s['metric']}={simulate_cpu_samples(s['base'], s['drift_amp'], s['jitter_std'], s['spike_val'], s['spike_prob'], t)['avg']}%"
                    for s in CPU_METRIC_SPECS
                )
            )

        print()
        time.sleep(WRITE_INTERVAL)

except KeyboardInterrupt:
    print("\nStopped.")
    client.close()