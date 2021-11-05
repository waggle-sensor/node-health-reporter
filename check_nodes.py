import argparse
import json
import multiprocessing
import subprocess
from urllib.request import urlopen
import os
import pandas as pd

def read_json_from_url(url):
    with urlopen(url) as f:
        return json.load(f)


def get_node_info_from_google_sheet(url):
    resp = read_json_from_url(url)
    df = pd.DataFrame(resp["values"], columns=["node", "online", "shield", "nxagent", "kind"])
    df["node"] = df["node"].str.lower()
    df["online"] = df["online"].str.lower() == "online"
    df["shield"] = df["shield"].str.lower() == "yes"
    df["nxagent"] = df["nxagent"].str.lower() == "yes"
    return df


def query_data(query):
    data = json.dumps(query).encode()
    with urlopen("https://data.sagecontinuum.org/api/v1/query", data) as f:
        df = pd.read_json(f, lines=True)
        if len(df) == 0:
            return pd.DataFrame({
            "timestamp": [],
            "name": [],
            "value": [],
            "meta.node": [],
            "meta.vsn": [],
        })
        meta = pd.json_normalize(df["meta"])
        meta.fillna("", inplace=True)
        meta.rename({c: "meta." + c for c in meta.columns}, axis="columns", inplace=True)
        df = df.join(meta)
        df.drop(columns=["meta"], inplace=True)
        return df


def check_ssh(node):
    try:
        subprocess.check_output(
            ["ssh", f"node-{node}", "true"], timeout=30, stderr=subprocess.DEVNULL
        )
        return (node, True)
    except Exception as exc:
        return (node, False)


def read_json_from_url(url):
    with urlopen(url) as f:
        return json.load(f)


def get_expected_plugins():
    resources_by_node = read_json_from_url("https://portal.sagecontinuum.org/ses-plugin-data/latest-status.json")
    return {node.lower(): {r["meta"]["deployment"] or "" for r in resources} for node, resources in resources_by_node.items()}


sys_from_nx = {
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

bme_names = {
    "env.temperature",
    "env.relative_humidity",
    "env.pressure",
}

raingauge_names = {
    "env.raingauge.acc",
    "env.raingauge.event_acc",
    "env.raingauge.rint",
    "env.raingauge.total_acc",
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output", default=None, help="output csv")
    parser.add_argument("--window", default="5m", help="time window to check")
    parser.add_argument("--ssh", action="store_true", default=False, help="include ssh check")
    parser.add_argument("--uploads", action="store_true", default=False, help="include uploads check")
    args = parser.parse_args()

    GOOGLE_SHEET_URL = os.environ["GOOGLE_SHEET_URL"]

    # TODO get the headers from spreadsheet dynamically
    node_info = get_node_info_from_google_sheet(GOOGLE_SHEET_URL)
    all_nodes = set(node_info.node)
    online_nodes = node_info[node_info.online].node
    offline_nodes = set(node_info[~node_info.online].node)
    expected_nodes_with_rpi = set(online_nodes[node_info.shield])
    expected_nodes_with_agent = set(online_nodes[node_info.nxagent])
    wsn_nodes = set(online_nodes[node_info.kind == "wsn"])
    blade_nodes = set(online_nodes[node_info.kind == "blade"])

    if args.ssh:
        with multiprocessing.Pool(8) as pool:
            ssh_status = dict(pool.map(check_ssh, online_nodes))

    results = []

    df = query_data(
        {
            "start": f"-{args.window}",
            "tail": 1,
        }
    )

    df.loc[df["meta.vsn"] == "", "meta.vsn"] = "W000"

    total_unexpected = 0

    nodes_checked = set()

    for (node, vsn), df_node in df.groupby(["meta.node", "meta.vsn"]):
        nodes_checked.add(node)

        if vsn == "W000":
            results.append({"node": node, "vsn": vsn, "msg": "!!! using invalid vsn"})
            continue

        # check if an unlisted node is sending data (query *could* return more nodes than in manifest)
        if node not in all_nodes:
            results.append({"node": node, "vsn": vsn, "msg": "!!! unlisted node is sending data"})
            total_unexpected += 1
            continue

        # check if node is unexpectedly sending data
        if node in offline_nodes:
            results.append(
                {"node": node, "vsn": vsn, "msg": "!!! node marked as offline is sending data"}
            )
            total_unexpected += 1
            continue

        # node is assumed to be online for rest of this section

        if node in wsn_nodes:
            # check nxcore sys.*
            found = set(df_node.loc[df_node["meta.host"].str.endswith("nxcore"), "name"])
            for name in sys_from_nx - found:
                results.append({"node": node, "vsn": vsn, "msg": f"missing nxcore {name}"})

            if node in expected_nodes_with_rpi:
                # check rpi sys.*
                found = set(df_node.loc[df_node["meta.host"].str.endswith("rpi"), "name"])
                for name in sys_from_rpi - found:
                    results.append({"node": node, "vsn": vsn, "msg": f"missing rpi {name}"})

            if node in expected_nodes_with_agent:
                # check nxagent sys.*
                found = set(df_node.loc[df_node["meta.host"].str.endswith("nxagent"), "name"])
                for name in sys_from_nxagent - found:
                    results.append({"node": node, "vsn": vsn, "msg": f"missing nxagent {name}"})

            # check bme280
            found = set(df_node[df_node["meta.sensor"] == "bme280"].name)
            for name in bme_names - found:
                results.append({"node": node, "vsn": vsn, "msg": f"missing bme280 {name}"})

            if node in expected_nodes_with_rpi:
                # check bme680
                found = set(df_node[df_node["meta.sensor"] == "bme680"].name)
                for name in bme_names - found:
                    results.append({"node": node, "vsn": vsn, "msg": f"missing bme680 {name}"})

                # check raingauge
                found = set(df_node.loc[:, "name"])
                for name in raingauge_names - found:
                    results.append({"node": node, "vsn": vsn, "msg": f"missing raingauge {name}"})
        elif node in blade_nodes:
            # check dellblade sys.*
            found = set(df_node.loc[df_node["meta.host"].str.endswith("dellblade"), "name"])
            for name in sys_from_dellblade - found:
                results.append({"node": node, "vsn": vsn, "msg": f"missing dellblade {name}"})

    if args.ssh:
        for node in set(online_nodes):
            # NOTE no vsn in data to match with, so use node
            if not ssh_status.get(node, False):
                results.append({"node": node, "vsn": node, "msg": "!!! node has no ssh connection"})

    # ensure all online nodes are accounted for
    missing_nodes = set(online_nodes) - nodes_checked
    for node in missing_nodes:
        # NOTE no vsn in data to match with
        results.append({"node": node, "vsn": node, "msg": f"!!! no data"})

    if args.uploads:
        # this is purely a test based on whether and upload exists in last 2h. we can make this more dynamic, if needed.
        df_uploads = query_data(
            {
                "start": "-2h",
                "tail": 1,
                "filter": {
                    "name": "upload"
                }
            }
        )

        # get set of all unique (node, task)
        uploads = set(df_uploads.groupby(["meta.node", "meta.task"]).groups.keys())

        # NOTE this should be moved to a more unified place
        node_to_vsn = dict(df_uploads.groupby(["meta.node", "meta.vsn"]).groups.keys())

        for node, plugins in get_expected_plugins().items():
            if node in offline_nodes:
                continue
            # TODO centralize where this is being determined
            if node in missing_nodes:
                continue
            for plugin in plugins:
                # NOTE eventually, plugins can contain some metadata on what their outputs will be. this will help eliminate this special case.
                if not "sampler" in plugin:
                    continue
                if (node, plugin) not in uploads:
                    results.append({"node": node, "vsn": node_to_vsn.get(node, node), "msg": f"missing upload from {plugin}"})

    results = pd.DataFrame(results)
    for (node, vsn), results_node in results.groupby(["node", "vsn"]):
        print(f"# {node} - {vsn}")
        print()
        for msg in sorted(results_node.msg):
            print(msg)
        print()

    if args.output is not None:
        results.sort_values(["node", "msg"]).to_csv(args.output, index=False)

    print()
    print("Total nodes listed as online:", len(online_nodes))
    print("Total nodes listed as offline:", len(offline_nodes))
    print("Total checked:", len(nodes_checked))
    print("Total missing:", len(missing_nodes))
    print("Total unexpected nodes:", total_unexpected)
    nodes_with_issues = set(results.node)
    print("Total nodes with issues:", len(nodes_with_issues))
    print("Total unique data series:", len(df))


if __name__ == "__main__":
    main()
