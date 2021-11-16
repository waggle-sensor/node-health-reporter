import sage_data_client
import pandas as pd
import influxdb_client
from influxdb_client.client.write_api import WriteOptions, WritePrecision, Point, WriteType
import os
import requests
import logging


INFLUXDB_TOKEN = os.environ["INFLUXDB_TOKEN"]


# probably can do this in simpler way using production list
def get_sanity_test_rollup_results(vsns, start, end, now=None):
    if now is None:
        now = pd.to_datetime("now", utc=True)
    
    start = (now + pd.to_timedelta(start)).floor("1h")
    end = (now + pd.to_timedelta(end)).floor("1h")

    # request data 30min before and after [start, end] window so we always
    # include data we would round in.
    df = sage_data_client.query(
        start=(start - pd.to_timedelta("30min")).isoformat(),
        end=(end + pd.to_timedelta("30min")).isoformat(),
        filter={
            "name": "sys.sanity.*"
        }
    )

    df["timestamp"] = df["timestamp"].dt.round("1h")
    df["total"] = 1
    df["pass"] = df["value"] == 0
    df["fail"] = df["value"] != 0

    table = pd.pivot_table(df, values=["total", "pass", "fail"], index="timestamp", columns=["meta.node", "meta.vsn"], aggfunc="sum", fill_value=0)

    # fill in all time windows
    index = pd.date_range(start.floor("1h"), end.floor("1h"), freq="h")
    table = table.reindex(index)

    # TODO need to filling in the vsn
    # groups = df.groupby("meta.vsn")
    
    # for vsn in vsns:
    #     try:
    #         df_vsn = groups.get_group(vsn)
    #         table_vsn = pd.pivot_table(df_vsn, values=["total", "pass", "fail"], index="timestamp", aggfunc="sum", fill_value=0)
    #         table_vsn = table_vsn.reindex(index, fill_value=0)
    #     except KeyError:
    #         table_vsn = pd.DataFrame([], index=index)
    #         table_vsn["total"] = 0
    #         table_vsn["pass"] = 0
    #         table_vsn["fail"] = 0
    #     print(table_vsn)
    #     print()

    results = []

    fieldnames = {
        "total": "sanity_test_total",
        "pass": "sanity_test_pass_total",
        "fail": "sanity_test_fail_total",
    }

    for (field, node, vsn, ts), total in table.unstack().iteritems():
        results.append({
            "timestamp": ts,
            "name": fieldnames[field],
            "vsn": vsn,
            "node": node,
            "value": total,
        })

    return results


def write_results_to_influxdb(results):
    data = []

    for item in results:
        data.append(Point(item["name"])
            .tag("vsn", item["vsn"]) \
            .tag("node", item["node"]) \
            .field("value", item["value"]) \
            .time(int(item["timestamp"].timestamp()), write_precision=WritePrecision.S))

    logging.info("writing %s records...", len(data))

    url = "https://influxdb.sagecontinuum.org"
    token = INFLUXDB_TOKEN
    org = "waggle"

    with influxdb_client.InfluxDBClient(url=url, token=token, org=org) as client, \
         client.write_api(write_options=WriteOptions(batch_size=10000)) as write_api:
        write_api.write(bucket="downsampled-test", org=org, record=data, write_precision=WritePrecision.S)

    logging.info("done!")

    # we can also use this to log various things in our infrastructure so they can be used when logging


def main():
    logging.basicConfig(level=logging.INFO)

    r = requests.get("https://api.sagecontinuum.org/production")
    production_list = r.json()
    vsns = {item["vsn"] for item in production_list if item["vsn"] != ""}

    results = []
    results.extend(get_sanity_test_rollup_results(vsns, "-4h", "-1h"))
    write_results_to_influxdb(results)


if __name__ == "__main__":
    main()

# honestly... this would probably be easier to put on a cronjob...
