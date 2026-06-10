"""
vsphere_cpu_collector.py
========================
Discovers and collects ALL vSphere CPU performance counters for a VM and writes
them to InfluxDB. Runs in a polling loop at the vSphere real-time interval (20s).

Dependencies:
    pip install pyVmomi pyVim influxdb-client typer rich

Counter discovery:
    On first run (or with the list-counters sub-command) the script queries
    vCenter for every counter in the "cpu" group and prints what it found.
    No hard-coded counter IDs — they vary between vCenter versions.

Instances:
    ""  (empty string)  = aggregate across all vCPUs
    "0", "1", ...       = per-vCPU data
    "*"                 = request all instances (aggregate + per-vCPU)

Test-data mode:
    Pass --test-data to skip vCenter entirely and generate realistic synthetic
    CPU metrics. Useful for developing the plotter without a vSphere environment.
"""

import math
import random
import ssl
import time
import logging
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from enum import Enum
from typing import Annotated, Optional, TypeAlias

import typer
from rich.console import Console
from rich.table import Table

from influxdb_client.client.influxdb_client import InfluxDBClient
from influxdb_client.client.write.point import Point
from influxdb_client.domain.write_precision import WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)
console = Console()

# ---------------------------------------------------------------------------
# Typer app
# ---------------------------------------------------------------------------

app = typer.Typer(
    name="vsphere-cpu-collector",
    help="Collect vSphere CPU metrics → InfluxDB",
    add_completion=False,
)

# ---------------------------------------------------------------------------
# Shared option types (reused across commands)
# ---------------------------------------------------------------------------

class InstanceMode(str, Enum):
    aggregate = "aggregate"   # "" in vSphere API
    all       = "all"         # "*" in vSphere API


# Common CLI option type aliases — defined once, reused across commands.
# TypeAlias lets Pyright treat these as valid type expressions in signatures.
InfluxUrlOpt:    TypeAlias = Annotated[str, typer.Option("--influx-url",    envvar="INFLUX_URL",    help="InfluxDB base URL")]
InfluxOrgOpt:    TypeAlias = Annotated[str, typer.Option("--influx-org",    envvar="INFLUX_ORG",    help="InfluxDB organisation")]
InfluxBucketOpt: TypeAlias = Annotated[str, typer.Option("--influx-bucket", envvar="INFLUX_BUCKET", help="InfluxDB bucket name")]
VcenterOpt:      TypeAlias = Annotated[str, typer.Option("--vcenter", envvar="VCENTER_HOST", help="vCenter hostname or IP")]
VcUserOpt:       TypeAlias = Annotated[str, typer.Option("--vc-user", envvar="VCENTER_USER", help="vCenter username")]
VcPortOpt:       TypeAlias = Annotated[int, typer.Option("--vc-port", envvar="VCENTER_PORT", help="vCenter port")]

# Standalone Annotated types for options that need prompt/hide_input.
# These carry ALL configuration inside Annotated so no default is needed.
VcPass = Annotated[str, typer.Option(
    "--vc-pass", envvar="VCENTER_PASSWORD",
    prompt="vCenter password", hide_input=True,
    help="vCenter password (or set VCENTER_PASSWORD)",
)]
InfluxToken = Annotated[str, typer.Option(
    "--influx-token", envvar="INFLUX_TOKEN",
    prompt="InfluxDB token", hide_input=True,
    help="InfluxDB auth token (or set INFLUX_TOKEN)",
)]

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class CpuCounter:
    key:       int
    group:     str
    name:      str
    rollup:    str
    unit:      str
    full_name: str = field(init=False)

    def __post_init__(self):
        self.full_name = f"{self.group}.{self.name}.{self.rollup}"


# ---------------------------------------------------------------------------
# vSphere helpers
# ---------------------------------------------------------------------------

def vcenter_connect(host: str, user: str, password: str, port: int = 443):
    """Return a connected ServiceInstance, ignoring self-signed certs."""
    from pyVmomi import vim  # type: ignore[import-not-found]  # noqa: F401
    from pyVim.connect import SmartConnect  # type: ignore[import-not-found]

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode    = ssl.CERT_NONE
    si = SmartConnect(host=host, user=user, pwd=password,
                      port=port, sslContext=context)
    log.info("Connected to vCenter: %s", host)
    return si


def get_vm_by_name(si, name: str):
    from pyVmomi import vim  # type: ignore[import-not-found]
    content   = si.RetrieveContent()
    container = content.viewManager.CreateContainerView(
        content.rootFolder, [vim.VirtualMachine], recursive=True
    )
    try:
        for vm in container.view:
            if vm.name == name:
                return vm
    finally:
        container.Destroy()
    return None


def discover_cpu_counters(si) -> list[CpuCounter]:
    """Return every counter in the 'cpu' group from this vCenter."""
    perf     = si.RetrieveContent().perfManager
    counters = [
        CpuCounter(
            key    = c.key,
            group  = c.groupInfo.key,
            name   = c.nameInfo.key,
            rollup = str(c.rollupType),
            unit   = str(c.unitInfo.key),
        )
        for c in perf.perfCounter
        if c.groupInfo.key == "cpu"
    ]
    log.info("Discovered %d CPU counters", len(counters))
    return counters


def query_cpu_metrics(
    si,
    vm,
    counters:    list[CpuCounter],
    instances:   str   = "*",
    interval_id: int   = 20,
    lookback_s:  int   = 60,
    vcenter_host: str  = "",
    influx_bucket: str = "",
) -> list[Point]:
    """Query all CPU counters and return InfluxDB Points."""
    from pyVmomi import vim  # type: ignore[import-not-found]

    perf_manager = si.RetrieveContent().perfManager
    now   = datetime.now(timezone.utc)
    start = now - timedelta(seconds=lookback_s)

    metric_ids = [
        vim.PerformanceManager.MetricId(counterId=c.key, instance=instances)
        for c in counters
    ]
    query = vim.PerformanceManager.QuerySpec(
        entity     = vm,
        metricId   = metric_ids,
        startTime  = start,
        endTime    = now,
        intervalId = interval_id,
        format     = "normal",
    )

    try:
        results = perf_manager.QueryPerf(querySpec=[query])
    except Exception as exc:
        log.error("QueryPerf failed: %s", exc)
        return []

    if not results:
        log.warning("QueryPerf returned no data for VM '%s'", vm.name)
        return []

    counter_by_id = {c.key: c for c in counters}
    points: list[Point] = []
    result = results[0]
    timestamps = [s.timestamp for s in result.sampleInfo]

    for series in result.value:
        counter = counter_by_id.get(series.id.counterId)
        if counter is None:
            continue
        instance_tag = series.id.instance if series.id.instance else "aggregate"
        scale        = 0.01 if counter.unit == "percent" else 1.0
        field_name   = f"{counter.name}_{counter.rollup}"

        for i, ts in enumerate(timestamps):
            raw = series.value[i]
            if raw == -1:
                continue
            p = (
                Point("vm_cpu")
                .tag("vm_name",  vm.name)
                .tag("vcenter",  vcenter_host)
                .tag("instance", instance_tag)
                .tag("unit",     counter.unit)
                .field(field_name, raw * scale)
                .time(ts, WritePrecision.S)
            )
            points.append(p)

    log.debug("Built %d points from vSphere query", len(points))
    return points


# ---------------------------------------------------------------------------
# Test-data generator
# ---------------------------------------------------------------------------

# Realistic CPU counter definitions for synthetic data.
# (name, rollup, unit, base_value, amplitude, noise_scale)
_SYNTHETIC_COUNTERS = [
    # Utilisation
    ("usage",      "average",    "percent",      35.0,  20.0,  3.0),
    ("usagemhz",   "average",    "megaHertz",  1400.0, 800.0, 50.0),
    ("demand",     "average",    "megaHertz",  1350.0, 750.0, 40.0),
    ("entitlement","latest",     "megaHertz",  2000.0,   0.0,  5.0),
    # Readiness / latency
    ("readiness",  "average",    "percent",       5.0,   4.0,  0.5),
    ("latency",    "average",    "percent",       3.0,   2.5,  0.3),
    ("costop",     "summation",  "millisecond",   2.0,   1.5,  0.2),
    ("overlap",    "summation",  "millisecond",   1.0,   0.8,  0.1),
    # Wait / idle
    ("ready",      "summation",  "millisecond",  80.0,  60.0,  8.0),
    ("wait",       "summation",  "millisecond", 120.0,  80.0, 10.0),
    ("idle",       "summation",  "millisecond", 800.0, 100.0, 20.0),
    ("vmwait",     "summation",  "millisecond",  10.0,   8.0,  1.0),
    ("swapwait",   "summation",  "millisecond",   0.5,   0.4,  0.05),
]

# Phase offsets so each metric has a slightly different wave shape
_PHASES = {name: random.uniform(0, 2 * math.pi) for name, *_ in _SYNTHETIC_COUNTERS}


def _synthetic_value(name: str, base: float, amp: float, noise: float, t: float) -> float:
    """
    Generate a value that oscillates realistically:
      base + sine wave + random walk noise, clamped to >= 0.
    """
    phase   = _PHASES[name]
    sine    = amp * 0.5 * (1 + math.sin(t * 0.3 + phase))
    jitter  = random.gauss(0, noise)
    return max(0.0, base + sine + jitter)


def generate_test_points(
    vm_name:       str,
    vcenter_host:  str,
    instances:     str,
    interval_s:    int,
    lookback_s:    int,
) -> list[Point]:
    """
    Build a batch of synthetic InfluxDB Points that mimic what a real
    vSphere query would return, covering the last `lookback_s` seconds.
    """
    now    = datetime.now(timezone.utc)
    start  = now - timedelta(seconds=lookback_s)
    step   = timedelta(seconds=interval_s)

    # Build list of sample timestamps
    sample_times: list[datetime] = []
    t = start
    while t <= now:
        sample_times.append(t)
        t += step

    # Determine which vCPU instances to emit
    vcpu_instances: list[str] = []
    if instances == "*":
        vcpu_instances = ["0", "1", "2", "3"]   # simulate a 4-vCPU VM

    points: list[Point] = []
    t_epoch = now.timestamp()   # single time reference for the sine wave

    for (name, rollup, unit, base, amp, noise) in _SYNTHETIC_COUNTERS:
        field_name = f"{name}_{rollup}"

        # Always emit an aggregate instance
        emit_instances = [("aggregate", 1.0)]

        # Per-vCPU: scale so they sum roughly to the aggregate
        n_vcpu = len(vcpu_instances)
        if n_vcpu:
            per_cpu_scale = 1.0 / n_vcpu
            emit_instances += [(cpu_id, per_cpu_scale) for cpu_id in vcpu_instances]

        for (instance_tag, scale) in emit_instances:
            # Slightly different phase per instance so they don't overlap exactly
            inst_jitter = hash(instance_tag) % 100 * 0.01
            for ts in sample_times:
                age = (now - ts).total_seconds()
                raw = _synthetic_value(
                    name,
                    base  * scale,
                    amp   * scale,
                    noise * scale,
                    t_epoch - age + inst_jitter,
                )
                p = (
                    Point("vm_cpu")
                    .tag("vm_name",  vm_name)
                    .tag("vcenter",  vcenter_host)
                    .tag("instance", instance_tag)
                    .tag("unit",     unit)
                    .field(field_name, round(raw, 4))
                    .time(ts, WritePrecision.S)
                )
                points.append(p)

    log.debug("Generated %d synthetic test points", len(points))
    return points


# ---------------------------------------------------------------------------
# InfluxDB writer
# ---------------------------------------------------------------------------

def write_to_influx(
    points: list[Point],
    client: InfluxDBClient,
    org:    str,
    bucket: str,
) -> None:
    if not points:
        log.warning("No points to write")
        return
    write_api = client.write_api(write_options=SYNCHRONOUS)
    write_api.write(bucket=bucket, org=org, record=points)
    log.info("Written %d points to InfluxDB bucket '%s'", len(points), bucket)


# ---------------------------------------------------------------------------
# Sub-commands
# ---------------------------------------------------------------------------

@app.command()
def collect(
    vm_name: Annotated[str, typer.Argument(help="Exact VM display name in vCenter")],

    # vSphere connection
    vcenter:  VcenterOpt  = "vcenter.example.com",
    vc_user:  VcUserOpt   = "administrator@vsphere.local",
    vc_pass:  VcPass      = None,   # type: ignore[assignment]  prompted if absent
    vc_port:  VcPortOpt   = 443,

    # InfluxDB connection
    influx_url:    InfluxUrlOpt    = "http://localhost:8086",
    influx_token:  InfluxToken     = None,   # type: ignore[assignment]  prompted if absent
    influx_org:    InfluxOrgOpt    = "my-org",
    influx_bucket: InfluxBucketOpt = "vsphere_metrics",

    # Collection behaviour
    interval: Annotated[int, typer.Option(
        "--interval", help="vSphere intervalId in seconds (20=real-time, 300=5-min rollup)"
    )] = 20,
    instances: Annotated[InstanceMode, typer.Option(
        "--instances", help="'aggregate' = VM total only; 'all' = aggregate + per-vCPU"
    )] = InstanceMode.aggregate,
    once: Annotated[bool, typer.Option(
        "--once", help="Run a single collection pass then exit"
    )] = False,

    # Test-data mode
    test_data: Annotated[bool, typer.Option(
        "--test-data", help="Generate synthetic CPU data instead of connecting to vCenter"
    )] = False,
):
    """
    Collect vSphere CPU metrics and write them to InfluxDB.

    Polls vCenter every INTERVAL seconds and writes all CPU counters to InfluxDB.
    Use --test-data to run without a real vCenter for development/testing.

    All sensitive options can also be supplied via environment variables:
    VCENTER_PASSWORD, INFLUX_TOKEN, INFLUX_URL, INFLUX_ORG, INFLUX_BUCKET, etc.
    """
    instance_api_str = "" if instances == InstanceMode.aggregate else "*"
    lookback_s       = max(interval * 3, 60)

    influx_client = InfluxDBClient(
        url=influx_url, token=influx_token, org=influx_org
    )

    if test_data:
        console.print(f"[bold yellow]⚠  TEST DATA MODE[/] — no vCenter connection")
        console.print(f"   VM name  : [cyan]{vm_name}[/]")
        console.print(f"   vCenter  : [dim]{vcenter}[/] (not used)")
        console.print(f"   Instances: [cyan]{instances.value}[/]")
        console.print(f"   InfluxDB : [cyan]{influx_url}[/]  bucket=[cyan]{influx_bucket}[/]")
        console.print()

        try:
            while True:
                points = generate_test_points(
                    vm_name      = vm_name,
                    vcenter_host = vcenter,
                    instances    = instance_api_str,
                    interval_s   = interval,
                    lookback_s   = lookback_s,
                )
                write_to_influx(points, influx_client, influx_org, influx_bucket)

                if once:
                    break

                log.info("Sleeping %ds …", interval)
                time.sleep(interval)
        finally:
            influx_client.close()
        return

    # ---- Real vCenter path ------------------------------------------------
    si = vcenter_connect(vcenter, vc_user, vc_pass, vc_port)
    try:
        counters = discover_cpu_counters(si)
        vm       = get_vm_by_name(si, vm_name)
        if vm is None:
            console.print(f"[bold red]Error:[/] VM '{vm_name}' not found in vCenter")
            raise typer.Exit(1)

        log.info("Targeting VM: %s  (moRef: %s)", vm.name, vm._moId)

        try:
            while True:
                points = query_cpu_metrics(
                    si            = si,
                    vm            = vm,
                    counters      = counters,
                    instances     = instance_api_str,
                    interval_id   = interval,
                    lookback_s    = lookback_s,
                    vcenter_host  = vcenter,
                    influx_bucket = influx_bucket,
                )
                write_to_influx(points, influx_client, influx_org, influx_bucket)

                if once:
                    break

                log.info("Sleeping %ds …", interval)
                time.sleep(interval)
        finally:
            influx_client.close()
    finally:
        from pyVim.connect import Disconnect  # type: ignore[import-not-found]
        Disconnect(si)
        log.info("Disconnected from vCenter")


@app.command(name="list-counters")
def list_counters(
    vcenter:  VcenterOpt = "vcenter.example.com",
    vc_user:  VcUserOpt  = "administrator@vsphere.local",
    vc_pass:  VcPass     = None,   # type: ignore[assignment]  prompted if absent
    vc_port:  VcPortOpt  = 443,

    test_data: Annotated[bool, typer.Option(
        "--test-data", help="Show the built-in synthetic counter list instead of querying vCenter"
    )] = False,
):
    """
    Print all CPU performance counters available in vCenter (or the synthetic list).
    """
    if test_data:
        table = Table(title="Synthetic CPU Counters", show_lines=True)
        table.add_column("Field name",  style="cyan")
        table.add_column("Unit",        style="green")
        table.add_column("Base value",  justify="right")
        for name, rollup, unit, base, amp, _ in _SYNTHETIC_COUNTERS:
            table.add_row(f"{name}_{rollup}", unit, str(base))
        console.print(table)
        return

    si = vcenter_connect(vcenter, vc_user, vc_pass, vc_port)
    try:
        counters = discover_cpu_counters(si)
        table    = Table(title="vCenter CPU Counters", show_lines=True)
        table.add_column("Key",       justify="right", style="dim")
        table.add_column("Full name", style="cyan")
        table.add_column("Unit",      style="green")
        for c in sorted(counters, key=lambda x: x.full_name):
            table.add_row(str(c.key), c.full_name, c.unit)
        console.print(table)
    finally:
        from pyVim.connect import Disconnect  # type: ignore[import-not-found]
        Disconnect(si)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app()