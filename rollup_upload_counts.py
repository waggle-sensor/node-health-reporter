import argparse
import os
from os import getenv
import pandas as pd
import logging
import sage_data_client

from utils import (
    load_node_table,
    parse_time,
    get_rollup_range,
    get_time_windows,
    write_results_to_influxdb,
)


def get_media_counts_for_window(nodes, start, end, convert_timestamps=False):
    df = sage_data_client.query(
        start=start,
        end=end,
        filter={"name": "upload"},
        experimental_func="count",
    )

    df["timestamp"] = df["timestamp"].dt.round("1h")
    # experimental_func count returns the total counts as the value field
    df["total"] = df["value"]

    cols = ["meta.node", "meta.vsn", "meta.plugin", "meta.task"]

    # 'meta.camera' doesn't exist for older uploads; only the newer image-sampler recoreds
    if "meta.camera" in df.columns:
        cols = cols + ["meta.camera"]

    table = df.groupby(cols)[["total"]].sum()

    records = []
    for node in nodes:
        try:
            r = table.loc[(node.id, node.vsn)]
            plugin_totals = r["total"]
        except KeyError:
            continue  # ignore if id & vsn combination not found

        for plugin_key, total in plugin_totals.items():
            if len(plugin_key) == 3:
                plugin_name, task, camera = plugin_key
            else:
                plugin_name, task = plugin_key
                camera = None

            record = {
                "measurement": "total",
                "tags": {
                    "vsn": node.vsn,
                    "node": node.id,
                    "plugin": plugin_name,
                    "task": task,
                    **({"camera": camera} if camera is not None else {})
                },
                "fields": {
                    "value": int(total),  # Ensure value is integer
                },
                "timestamp": (
                    start.isoformat() + "Z" if convert_timestamps else start
                ),
            }

            records.append(record)


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
        "--start", default="-1h", type=time_arg, help="relative start time"
    )
    parser.add_argument("--end", default="now", type=time_arg, help="relative end time")
    parser.add_argument(
        "--window",
        default="1h",
        type=pd.Timedelta,
        help="window duration to aggreagate over",
    )
    parser.add_argument(
        "--reverse",
        action="store_true",
        help="reverse the rollup starting so it works from most recent to least recent",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%Y/%m/%d %H:%M:%S",
    )

    if not args.dry_run:
        INFLUXDB_URL = getenv("INFLUXDB_URL", "https://influxdb.sagecontinuum.org")
        INFLUXDB_ORG = getenv("INFLUXDB_ORG", "waggle")
        INFLUXDB_TOKEN = os.environ["INFLUXDB_TOKEN"]
        INFLUXDB_BUCKET = getenv("INFLUXDB_BUCKET", "upload-stats")

    nodes = load_node_table()
    start, end = get_rollup_range(args.start, args.end)
    window = args.window

    logging.info("current time is %s", now)

    time_windows = get_time_windows(start, end, window)

    if args.reverse:
        time_windows = reversed(time_windows)

    for start, end in time_windows:
        logging.info("getting upload counts for %s %s", start, end)
        records = get_media_counts_for_window(nodes, start, end)

        if not args.dry_run:
            logging.info("writing %d upload count records...", len(records))
            write_results_to_influxdb(
                url=INFLUXDB_URL,
                org=INFLUXDB_ORG,
                token=INFLUXDB_TOKEN,
                bucket=INFLUXDB_BUCKET,
                records=records,
            )

    logging.info("done!")


if __name__ == "__main__":
    main()
