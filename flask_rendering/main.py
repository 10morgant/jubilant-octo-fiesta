from flask import Flask, Response, render_template, request
from influxdb_client.client.influxdb_client import InfluxDBClient
import json, time

app = Flask(__name__)

INFLUX_URL    = "http://localhost:8086"
INFLUX_TOKEN  = "vm-perf-admin-token"
INFLUX_ORG    = "vm_perf"
INFLUX_BUCKET = "vm_perf"

client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
query_api = client.query_api()

# ---------------------------------------------------------------------------
# CPU config — one chart per metric, three togglable series (avg / min / max)
# ---------------------------------------------------------------------------

# Allowed (range, aggregation window) pairs — keyed by the range string
# Wider ranges use larger windows to keep point counts manageable
RANGES = {
    "-5m":  "5s",
    "-15m": "10s",
    "-30m": "30s",
    "-1h":  "1m",
    "-3h":  "2m",
    "-6h":  "5m",
    "-12h": "10m",
}
DEFAULT_RANGE = "-15m"

# Shared colour palette: avg=purple, min=green, max=red
_AVG = "#a78bfa"
_MIN = "#4ade80"
_MAX = "#f87171"

CPU_METRICS = [
    {
        "id":    "usage",
        "label": "CPU Usage (%)",
        "series": [
            {"field": "usage_avg", "agg": "avg", "label": "Average", "colour": _AVG},
            {"field": "usage_min", "agg": "min", "label": "Min",     "colour": _MIN},
            {"field": "usage_max", "agg": "max", "label": "Max",     "colour": _MAX},
        ],
    },
    {
        "id":    "readiness",
        "label": "CPU Readiness — ready but not scheduled (%)",
        "series": [
            {"field": "readiness_avg", "agg": "avg", "label": "Average", "colour": _AVG},
            {"field": "readiness_min", "agg": "min", "label": "Min",     "colour": _MIN},
            {"field": "readiness_max", "agg": "max", "label": "Max",     "colour": _MAX},
        ],
    },
]

# ---------------------------------------------------------------------------
# Data fetcher
# ---------------------------------------------------------------------------

def fetch_cpu(cpu_range, cpu_window):
    field_map = {}
    for m in CPU_METRICS:
        for s in m["series"]:
            field_map[s["field"]] = (m["id"], s["agg"])

    fields_filter = " or ".join(f'r._field == "{f}"' for f in field_map)
    query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: {cpu_range})
          |> filter(fn: (r) => r._measurement == "cpu")
          |> filter(fn: (r) => {fields_filter})
          |> group(columns: ["_field"])
          |> aggregateWindow(every: {cpu_window}, fn: mean, createEmpty: false)
    '''
    tables = query_api.query(query)
    result = {m["id"]: {s["agg"]: [] for s in m["series"]} for m in CPU_METRICS}
    for table in tables:
        for record in table.records:
            field = record.get_field()
            if field in field_map:
                metric_id, agg = field_map[field]
                result[metric_id][agg].append({
                    "time":  record.get_time().isoformat(),
                    "value": record.get_value(),
                })
    return result

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("cpu.html", metrics=CPU_METRICS, ranges=RANGES, default_range=DEFAULT_RANGE)


@app.route("/stream")
def stream():
    chosen = request.args.get("range", DEFAULT_RANGE)
    if chosen not in RANGES:
        chosen = DEFAULT_RANGE
    cpu_window = RANGES[chosen]

    def event_generator():
        while True:
            yield f"data: {json.dumps(fetch_cpu(chosen, cpu_window))}\n\n"
            time.sleep(5)
    return Response(event_generator(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


if __name__ == "__main__":
    app.run(debug=True, threaded=True)

INFLUX_URL    = "http://localhost:8086"
INFLUX_TOKEN  = "vm-perf-admin-token"
INFLUX_ORG    = "vm_perf"
INFLUX_BUCKET = "vm_perf"

client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
query_api = client.query_api()

# ---------------------------------------------------------------------------
# Latency config
# ---------------------------------------------------------------------------

# Each entry: (field, label, hex_colour)
LATENCY_SERIES = [
    ("p50",  "p50 latency (ms)",  "#60a5fa"),
    ("p95",  "p95 latency (ms)",  "#fbbf24"),
    ("p99",  "p99 latency (ms)",  "#f87171"),
]

# ---------------------------------------------------------------------------
# CPU config — one chart per metric, three togglable series (avg / min / max)
# ---------------------------------------------------------------------------

CPU_RANGE  = "-15m"
CPU_WINDOW = "30s"

CPU_METRICS = [
    {
        "id":    "usage",
        "label": "CPU Usage (%)",
        "series": [
            {"field": "usage_avg",     "agg": "avg", "label": "Average", "colour": "#60a5fa"},
            {"field": "usage_min",     "agg": "min", "label": "Min",     "colour": "#4ade80"},
            {"field": "usage_max",     "agg": "max", "label": "Max",     "colour": "#f87171"},
        ],
    },
    {
        "id":    "readiness",
        "label": "CPU Readiness (%)",
        "series": [
            {"field": "readiness_avg", "agg": "avg", "label": "Average", "colour": "#a78bfa"},
            {"field": "readiness_min", "agg": "min", "label": "Min",     "colour": "#34d399"},
            {"field": "readiness_max", "agg": "max", "label": "Max",     "colour": "#fb923c"},
        ],
    },
    {
        "id":    "wait",
        "label": "CPU Wait (%)",
        "series": [
            {"field": "wait_avg",      "agg": "avg", "label": "Average", "colour": "#e879f9"},
            {"field": "wait_min",      "agg": "min", "label": "Min",     "colour": "#2dd4bf"},
            {"field": "wait_max",      "agg": "max", "label": "Max",     "colour": "#fbbf24"},
        ],
    },
]

# ---------------------------------------------------------------------------
# Data fetchers
# ---------------------------------------------------------------------------

def fetch_latency():
    fields = " or ".join(f'r._field == "{s[0]}"' for s in LATENCY_SERIES)
    query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -5m)
          |> filter(fn: (r) => r._measurement == "latency")
          |> filter(fn: (r) => {fields})
          |> aggregateWindow(every: 5s, fn: mean)
          |> yield(name: "mean")
    '''
    tables = query_api.query(query)
    result = {s[0]: [] for s in LATENCY_SERIES}
    for table in tables:
        for record in table.records:
            field = record.get_field()
            if field in result:
                result[field].append({
                    "time":  record.get_time().isoformat(),
                    "value": record.get_value()
                })
    return result


def fetch_cpu():
    # Build field → (metric_id, agg) lookup
    field_map = {}
    for m in CPU_METRICS:
        for s in m["series"]:
            field_map[s["field"]] = (m["id"], s["agg"])

    fields_filter = " or ".join(f'r._field == "{f}"' for f in field_map)
    query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: {CPU_RANGE})
          |> filter(fn: (r) => r._measurement == "cpu")
          |> filter(fn: (r) => {fields_filter})
          |> group(columns: ["_field"])
          |> aggregateWindow(every: {CPU_WINDOW}, fn: mean, createEmpty: false)
    '''
    tables = query_api.query(query)
    result = {m["id"]: {s["agg"]: [] for s in m["series"]} for m in CPU_METRICS}
    for table in tables:
        for record in table.records:
            field = record.get_field()
            if field in field_map:
                metric_id, agg = field_map[field]
                result[metric_id][agg].append({
                    "time":  record.get_time().isoformat(),
                    "value": record.get_value(),
                })
    return result

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html", series=LATENCY_SERIES)


@app.route("/stream")
def stream():
    def event_generator():
        while True:
            yield f"data: {json.dumps(fetch_latency())}\n\n"
            time.sleep(5)
    return Response(event_generator(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/cpu")
def cpu():
    return render_template("cpu.html", metrics=CPU_METRICS)


@app.route("/stream/cpu")
def stream_cpu():
    def event_generator():
        while True:
            yield f"data: {json.dumps(fetch_cpu())}\n\n"
            time.sleep(5)
    return Response(event_generator(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


if __name__ == "__main__":
    app.run(debug=True, threaded=True)