import argparse
import os
import pandas as pd
import logging
import sage_data_client
import requests
from typing import NamedTuple
from utils import (
    load_node_table,
    parse_time,
    get_rollup_range,
    time_windows,
    write_results_to_influxdb,
    check_publishing_frequency,
)


sys_from_nxcore = {
    "sys.boot_time",
    "sys.cooling",
    "sys.cooling_max",
    "sys.cpu_seconds",
    "sys.freq.ape",
    "sys.freq.cpu",
    "sys.freq.cpu_max",
    "sys.freq.cpu_min",
    "sys.freq.cpu_perc",
    "sys.freq.emc",
    "sys.freq.emc_max",
    "sys.freq.emc_min",
    "sys.freq.emc_perc",
    "sys.freq.gpu",
    "sys.freq.gpu_max",
    "sys.freq.gpu_min",
    "sys.freq.gpu_perc",
    "sys.fs.avail",
    "sys.fs.size",
    "sys.hwmon",
    "sys.load1",
    "sys.load15",
    "sys.load5",
    "sys.mem.avail",
    "sys.mem.free",
    "sys.mem.total",
    "sys.net.rx_bytes",
    "sys.net.rx_packets",
    "sys.net.tx_bytes",
    "sys.net.tx_packets",
    "sys.net.up",
    "sys.power",
    "sys.rssh_up",
    "sys.thermal",
    "sys.time",
    "sys.uptime",
    # "sys.gps.lat", # not sent with no GPS fix
    # "sys.gps.lon", # not sent with no GPS fix
    # "sys.gps.alt", # not sent with no GPS fix
    # "sys.gps.epx", # not sent with no GPS fix
    # "sys.gps.epy", # not sent with no GPS fix
    # "sys.gps.epv", # not sent with no GPS fix
    # "sys.gps.satellites", # not sent with no GPS fix
    "sys.gps.mode",
}

sys_from_dellblade = {
    "sys.boot_time",
    # "sys.cooling",
    # "sys.cooling_max",
    "sys.cpu_seconds",
    # "sys.freq.ape",
    # "sys.freq.cpu",
    # "sys.freq.cpu_max",
    # "sys.freq.cpu_min",
    # "sys.freq.cpu_perc",
    # "sys.freq.emc",
    # "sys.freq.emc_max",
    # "sys.freq.emc_min",
    # "sys.freq.emc_perc",
    # "sys.freq.gpu",
    # "sys.freq.gpu_max",
    # "sys.freq.gpu_min",
    # "sys.freq.gpu_perc",
    "sys.fs.avail",
    "sys.fs.size",
    # "sys.hwmon",
    "sys.load1",
    "sys.load15",
    "sys.load5",
    "sys.mem.avail",
    "sys.mem.free",
    "sys.mem.total",
    "sys.net.rx_bytes",
    "sys.net.rx_packets",
    "sys.net.tx_bytes",
    "sys.net.tx_packets",
    "sys.net.up",
    # "sys.power",
    # "sys.rssh_up", # no network watchdog
    # "sys.thermal",
    "sys.time",
    "sys.uptime",
}

sys_from_nxagent = {
    "sys.boot_time",
    "sys.cooling",
    "sys.cooling_max",
    "sys.cpu_seconds",
    "sys.freq.ape",
    "sys.freq.cpu",
    "sys.freq.cpu_max",
    "sys.freq.cpu_min",
    "sys.freq.cpu_perc",
    "sys.freq.emc",
    "sys.freq.emc_max",
    "sys.freq.emc_min",
    "sys.freq.emc_perc",
    "sys.freq.gpu",
    "sys.freq.gpu_max",
    "sys.freq.gpu_min",
    "sys.freq.gpu_perc",
    "sys.fs.avail",
    "sys.fs.size",
    "sys.hwmon",
    "sys.load1",
    "sys.load15",
    "sys.load5",
    "sys.mem.avail",
    "sys.mem.free",
    "sys.mem.total",
    "sys.net.rx_bytes",
    "sys.net.rx_packets",
    "sys.net.tx_bytes",
    "sys.net.tx_packets",
    "sys.net.up",
    "sys.power",
    "sys.thermal",
    "sys.time",
    "sys.uptime",
}

sys_from_rpi = {
    "sys.boot_time",
    "sys.cpu_seconds",
    "sys.freq.cpu",
    "sys.freq.cpu_max",
    "sys.freq.cpu_min",
    "sys.freq.cpu_perc",
    "sys.fs.avail",
    "sys.fs.size",
    "sys.hwmon",
    "sys.load1",
    "sys.load15",
    "sys.load5",
    "sys.mem.avail",
    "sys.mem.free",
    "sys.mem.total",
    "sys.net.rx_bytes",
    "sys.net.rx_packets",
    "sys.net.tx_bytes",
    "sys.net.tx_packets",
    "sys.net.up",
    "sys.thermal",
    "sys.time",
    "sys.uptime",
}

outputs_from_bme = {
    "env.temperature",
    "env.relative_humidity",
    "env.pressure",
}

outputs_from_raingauge = {
    # "env.raingauge.acc",
    "env.raingauge.event_acc",
    "env.raingauge.rint",
    "env.raingauge.total_acc",
}

# device_output_table describes the output publishing policy for each of
# the possible devices on a node. the frequency is the minimum expected
# publishing frequency
device_output_table = {
    "nxcore": [("sys", name, "120s") for name in sys_from_nxcore],
    "nxagent": [("sys", name, "120s") for name in sys_from_nxagent],
    "rpi": [("sys", name, "120s") for name in sys_from_rpi],
    "dell": [("sys", name, "60s") for name in sys_from_dellblade],
    "bme280": [("wes-iio-bme280", name, "30s") for name in outputs_from_bme],
    "bme680": [("wes-iio-bme680", name, "30s") for name in outputs_from_bme],
    "raingauge": [("wes-raingauge", name, "30s") for name in outputs_from_raingauge],
    "top_camera": [("imagesampler-top", "upload", "1h")],
    "bottom_camera": [("imagesampler-bottom", "upload", "1h")],
    "left_camera": [("imagesampler-left", "upload", "1h")],
    "right_camera": [("imagesampler-right", "upload", "1h")],
    "microphone": [("audiosampler", "upload", "1h")],
}


def get_scheduled_tasks_by_node():
    """
    Queries the cloud scheduler and returns a map of VSN -> [Plugin names across all running jobs for VSN]
    """
    r = requests.get("https://es.sagecontinuum.org/api/v1/jobs/list")
    r.raise_for_status()
    jobs = list(r.json().values())

    # filter only running jobs
    jobs = [job for job in jobs if job["state"]["last_state"] == "Running"]

    tasks_by_node = {}

    for job in jobs:
        plugins = job.get("plugins") or []
        nodes = job.get("nodes") or {}

        for plugin in plugins:
            for vsn in nodes.keys():
                if vsn not in tasks_by_node:
                    tasks_by_node[vsn] = []
                tasks_by_node[vsn].append(plugin["name"])

    return tasks_by_node


def get_health_records_for_window(nodes, start, end, window):
    records = []

    logging.info("querying data...")
    df = sage_data_client.query(start=start, end=end)
    logging.info("done")

    logging.info("checking data...")

    timestamp = start

    def add_node_health_check_record(vsn, value):
        records.append(
            {
                "measurement": "node_health_check",
                "tags": {
                    "vsn": vsn,
                },
                "fields": {
                    "value": int(value),
                },
                "timestamp": timestamp,
            }
        )

    def add_device_health_check_record(vsn, device, value):
        records.append(
            {
                "measurement": "device_health_check",
                "tags": {
                    "vsn": vsn,
                    "device": device,
                },
                "fields": {
                    "value": int(value),
                },
                "timestamp": timestamp,
            }
        )

    # NOTE metrics agent doesn't add a task name, so we set task name
    # to system for system metrics.
    df.loc[df["name"].str.startswith("sys."), "meta.task"] = "sys"

    vsn_groups = df.groupby(["meta.vsn"])

    scheduled_tasks_by_node = get_scheduled_tasks_by_node()

    for node in nodes:
        try:
            df_vsn = vsn_groups.get_group(node.vsn)
        except:
            add_node_health_check_record(node.vsn, 0)
            for device in node.devices:
                add_device_health_check_record(node.vsn, device, 0)
            continue

        groups = df_vsn.groupby(["meta.task", "name"])

        def check_publishing_frequency_for_device(device, window):
            for task, name, freq in device_output_table[device]:
                try:
                    group = groups.get_group((task, name))
                    yield task, name, check_publishing_frequency(group, freq, window)
                except KeyError:
                    yield task, name, 0.0

        scheduled_tasks = scheduled_tasks_by_node.get(node.vsn, [])

        def check_publishing_sla_for_device(device, window, sla):
            healthy = True

            for task, name, f in check_publishing_frequency_for_device(device, window):
                # skip image and audio sampler tasks which are not scheduled
                if "sampler" in task and task not in scheduled_tasks:
                    continue
                if f < sla:
                    healthy = False
                    logging.info(
                        "failed sla %s %s %s %s %s %s %0.3f",
                        start,
                        end,
                        node.vsn,
                        device,
                        task,
                        name,
                        f,
                    )
            return healthy

        node_healthy = True

        for device in node.devices:
            # the idea here is to translate the publishing frequency into a kind of SLA. here
            # we're saying that after breaking the series up into window the size of the publishing
            # frequency, we should see 1 sample per window in 90% of the windows.
            healthy = check_publishing_sla_for_device(device, window, 0.90)
            # accumulate full node health
            node_healthy = node_healthy and healthy
            add_device_health_check_record(node.vsn, device, healthy)

        add_node_health_check_record(node.vsn, node_healthy)

    logging.info("done")

    return records


exclude_sanity_tests = [
    "sys.sanity_status.wes_telegraf_cadvisor",
]


def get_sanity_records_for_window(nodes, start, end):
    df = sage_data_client.query(start=start, end=end, filter={"name": "sys.sanity.*"})

    # drop excluded sanity tests we know are failing because of system changes
    df = df[~df.name.isin(exclude_sanity_tests)]

    df["timestamp"] = df["timestamp"].dt.round("1h")
    df["total"] = 1
    df["pass"] = (df["value"] == 0) | (df["meta.severity"] == "warning")
    df["fail"] = ~df["pass"]

    table = df.groupby(["meta.node", "meta.vsn"])[["total", "pass", "fail"]].sum()

    records = []

    for node in nodes:
        try:
            r = table.loc[(node.id, node.vsn)]
            totals = {
                "sanity_test_total": r["total"],
                "sanity_test_pass_total": r["pass"],
                "sanity_test_fail_total": r["fail"],
            }
        except KeyError:
            totals = {
                "sanity_test_total": 0,
                "sanity_test_pass_total": 0,
                "sanity_test_fail_total": 0,
            }

        for name, value in totals.items():
            records.append(
                {
                    "measurement": name,
                    "tags": {
                        "vsn": node.vsn,
                        "node": node.id,
                    },
                    "fields": {
                        "value": int(value),
                    },
                    "timestamp": start,
                }
            )

    return records


def main():
    now = pd.to_datetime("now", utc=True)

    def time_arg(s):
        return parse_time(s, now=now)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="perform dry run to view logs. will skip writing results to influxdb.",
    )
    parser.add_argument(
        "--start", default="-2h", type=time_arg, help="relative start time"
    )
    parser.add_argument("--end", default="-1h", type=time_arg, help="relative end time")
    parser.add_argument(
        "--window",
        default="1h",
        type=pd.Timedelta,
        help="window duration to aggreagate over",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%Y/%m/%d %H:%M:%S",
    )

    if not args.dry_run:
        INFLUXDB_URL = "https://influxdb.sagecontinuum.org"
        INFLUXDB_ORG = "waggle"
        INFLUXDB_TOKEN = os.environ["INFLUXDB_TOKEN"]

    nodes = load_node_table()
    start, end = get_rollup_range(args.start, args.end)
    window = args.window

    logging.info("current time is %s", now)

    for start, end in time_windows(start, end, window):
        logging.info("getting health records in %s %s", start, end)
        health_records = get_health_records_for_window(nodes, start, end, window)

        if not args.dry_run:
            logging.info("writing %d health records...", len(health_records))
            write_results_to_influxdb(
                url=INFLUXDB_URL,
                org=INFLUXDB_ORG,
                token=INFLUXDB_TOKEN,
                bucket="health-check-test",
                records=health_records,
            )

        logging.info("getting sanity records in %s %s", start, end)
        sanity_records = get_sanity_records_for_window(nodes, start, end)

        if not args.dry_run:
            logging.info("writing %d sanity health records...", len(sanity_records))
            write_results_to_influxdb(
                url=INFLUXDB_URL,
                org=INFLUXDB_ORG,
                token=INFLUXDB_TOKEN,
                bucket="downsampled-test",
                records=sanity_records,
            )

    logging.info("done!")


if __name__ == "__main__":
    main()
