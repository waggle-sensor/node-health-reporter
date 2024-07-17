import influxdb_client
from influxdb_client.client.write_api import (
    WriteOptions,
    WritePrecision,
    Point,
    WriteType,
)
import pandas as pd
import requests
from dataclasses import dataclass


def write_results_to_influxdb(url, token, org, bucket, records):
    data = []

    for r in records:
        p = Point(r["measurement"])
        for k, v in r["tags"].items():
            p = p.tag(k, v)
        for k, v in r["fields"].items():
            p = p.field(k, v)
        p = p.time(int(r["timestamp"].timestamp()), write_precision=WritePrecision.S)
        data.append(p)

    with influxdb_client.InfluxDBClient(
        url=url, token=token, org=org
    ) as client, client.write_api(
        write_options=WriteOptions(batch_size=10000)
    ) as write_api:
        write_api.write(
            bucket=bucket, org=org, record=data, write_precision=WritePrecision.S
        )


def check_publishing_frequency(df, freq, window):
    total_samples = (df.resample(freq, on="timestamp").value.count() > 0).sum()
    expected_samples = window / pd.Timedelta(freq)
    return total_samples / expected_samples


@dataclass
class Node:
    id: str
    vsn: str
    type: str
    devices: set


def load_node_table():
    r = requests.get("https://api.sagecontinuum.org/production")
    r.raise_for_status()
    return [load_node_table_item(item) for item in r.json() if item["vsn"] != ""]


def load_node_table_item(item):
    node_type = item["node_type"].lower()
    devices = set()
    if node_type == "wsn":
        devices.add("nxcore")
        devices.add("bme280")
    if node_type == "dell":
        devices.add("dell")
    if item["nx_agent"] is True:
        devices.add("nxagent")
    if item["shield"] is True:
        devices.add("rpi")
        devices.add("raingauge")
        devices.add("bme680")
        devices.add("microphone")

    # add cameras
    for dir in ["top", "bottom", "left", "right"]:
        if item[f"{dir}_camera"] not in [None, "", "none"]:
            devices.add(f"{dir}_camera")

    # TODO add camera stuff for upload checks
    return Node(
        id=item["node_id"].lower(),
        vsn=item["vsn"].upper(),
        type=node_type,
        devices=devices,
    )


def get_time_windows(start, end, freq):
    windows = pd.date_range(start, end, freq=freq)
    return list(zip(windows[:-1], windows[1:]))


def parse_time(s, now=None):
    try:
        return pd.to_datetime(s, utc=True)
    except ValueError:
        pass
    if now is None:
        now = pd.to_datetime("now", utc=True)
    try:
        return now + pd.to_timedelta(s)
    except ValueError:
        pass
    raise ValueError("invalid time format")


def get_rollup_range(start, end, now=None):
    if now is None:
        now = pd.to_datetime("now", utc=True)
    return start.floor("1h"), end.floor("1h")
