# Node Health / Sanity Rollup Job

This repo contains a few tools for running various health and sanity metric rollups such as:

* rollup_health_and_sanity_metrics.py: Rolls up health / sanity metrics into hourly windows.
* rollup_plugin_counts.py: Rolls up plugin counts into hourly windows.
* check_nodes.py: Creates issues CSV used by report_results.py.
* report_results.py: Report recent changes in check_nodes.py report to Slack.

## Manually dry run health / sanity rollup

First, make sure you've installed the dependencies by running:

```sh
pip3 install -r requirements.txt --upgrade
```

rollup_health_and_sanity_metrics.py accepts a --dry-run flag which can be used to verify the results that would be written to InfluxDB without actually writing them.

As an example, we can do a dry run of last 4 hours using:

```sh
python3 rollup_health_and_sanity_metrics.py --dry-run --start=-4h
```

This will provide detailed logs of the time window being aggergated as long with items that have failed to meet their SLA:

```txt
2022/11/07 15:41:12 current time is 2022-11-07 21:41:11.936215+00:00
2022/11/07 15:41:12 getting health records in 2022-11-07 17:00:00+00:00 2022-11-07 18:00:00+00:00
2022/11/07 15:41:12 querying data...
2022/11/07 15:41:17 done
2022/11/07 15:41:17 checking data...
2022/11/07 15:41:17 failed sla 2022-11-07 17:00:00+00:00 2022-11-07 18:00:00+00:00 W01C microphone audiosampler upload 0.000
2022/11/07 15:41:17 failed sla 2022-11-07 17:00:00+00:00 2022-11-07 18:00:00+00:00 W01E nxcore sys sys.cooling_max 0.000
2022/11/07 15:41:17 failed sla 2022-11-07 17:00:00+00:00 2022-11-07 18:00:00+00:00 W01E nxcore sys sys.freq.gpu_max 0.000
2022/11/07 15:41:17 failed sla 2022-11-07 17:00:00+00:00 2022-11-07 18:00:00+00:00 W01E nxcore sys sys.freq.cpu 0.000
2022/11/07 15:41:17 failed sla 2022-11-07 17:00:00+00:00 2022-11-07 18:00:00+00:00 W01E nxcore sys sys.freq.cpu_perc 0.000
2022/11/07 15:41:17 failed sla 2022-11-07 17:00:00+00:00 2022-11-07 18:00:00+00:00 W01E nxcore sys sys.load15 0.000
2022/11/07 15:41:17 failed sla 2022-11-07 17:00:00+00:00 2022-11-07 18:00:00+00:00 W01E nxcore sys sys.uptime 0.000
2022/11/07 15:41:17 failed sla 2022-11-07 17:00:00+00:00 2022-11-07 18:00:00+00:00 W01E nxcore sys sys.freq.emc_max 0.000
2022/11/07 15:41:17 failed sla 2022-11-07 17:00:00+00:00 2022-11-07 18:00:00+00:00 W01E nxcore sys sys.hwmon 0.000
2022/11/07 15:41:17 failed sla 2022-11-07 17:00:00+00:00 2022-11-07 18:00:00+00:00 W01E nxcore sys sys.net.tx_packets 0.000
2022/11/07 15:41:17 failed sla 2022-11-07 17:00:00+00:00 2022-11-07 18:00:00+00:00 W01E nxcore sys sys.freq.emc_min 0.000
2022/11/07 15:41:17 failed sla 2022-11-07 17:00:00+00:00 2022-11-07 18:00:00+00:00 W01E nxcore sys sys.net.rx_packets 0.000
2022/11/07 15:41:17 failed sla 2022-11-07 17:00:00+00:00 2022-11-07 18:00:00+00:00 W01E nxcore sys sys.fs.avail 0.000
2022/11/07 15:41:17 failed sla 2022-11-07 17:00:00+00:00 2022-11-07 18:00:00+00:00 W01E nxcore sys sys.freq.emc 0.000
2022/11/07 15:41:17 failed sla 2022-11-07 17:00:00+00:00 2022-11-07 18:00:00+00:00 W01E nxcore sys sys.fs.size 0.000
2022/11/07 15:41:17 failed sla 2022-11-07 17:00:00+00:00 2022-11-07 18:00:00+00:00 W01E nxcore sys sys.cpu_seconds 0.000
    ^log timestamp                   ^window start             ^window end         ^vsn  ^group^    ^metric        ^score (0 = all failed, 1 = all passed)
```

Note: Most of the SLAs are based soley on the existance of a particular metric. We generally do not check specific ranges of values in the rollup.
