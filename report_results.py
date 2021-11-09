import argparse
import os
from pathlib import Path
import shutil
import subprocess
import time
import pandas as pd
import slack


def publish_results_to_slack(result_file, save_file, token):
    if os.path.exists(save_file):
        olddf = pd.read_csv(filepath_or_buffer=save_file)
    else:
        olddf = pd.DataFrame(columns=["node", "msg"])

    newdf = pd.read_csv(filepath_or_buffer=result_file)
    mergedf = olddf.merge(newdf, how="outer", indicator="which", sort=True)
    fixeddf = mergedf[mergedf.which == "left_only"].drop(columns="which")
    brokendf = mergedf[mergedf.which == "right_only"].drop(columns="which")
    samedf = mergedf[mergedf.which == "both"].drop(columns="which")

    slack_blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "Data Pipeline Results", "emoji": True},
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "The Data Pipeline Checker analyzes the data uploaded from the nodes, compares against an expected set and reports differences.\n\n_Note: the Data Pipeline Checker runs continuously but only reports when there is a difference since the last report._",
            },
        },
        {"type": "divider"},
    ]
    total = 0
    total_same = 0
    total_new = 0
    total_fixed = 0
    for node, df_node in mergedf.groupby("node"):
        vsn = list(mergedf[mergedf.node == node]["vsn"])[0]
        same_cnt = samedf[samedf.node == node].count()["node"]
        new_cnt = brokendf[brokendf.node == node].count()["node"]
        fixed_cnt = fixeddf[fixeddf.node == node].count()["node"]
        total += same_cnt + new_cnt
        total_same += same_cnt
        total_new += new_cnt
        total_fixed += fixed_cnt

        slack_blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{vsn}* ({node})\n```{same_cnt+new_cnt} active issues ({same_cnt} recurring | {new_cnt} new)\n{fixed_cnt} resolved issues```",
                },
                "accessory": {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Node Status", "emoji": True},
                    "value": f"status_{node}",
                    "url": f"https://admin.sagecontinuum.org/node/{node}",
                    "action_id": "button-action",
                },
            }
        )

    slack_blocks.append({"type": "divider"})
    slack_blocks.append(
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Active Summary*\n\n```{total} active issues ({total_same} recurring | {total_new} new)\n{total_fixed} resolved issues```",
            },
        },
    )

    client = slack.WebClient(token=token)
    print("posting report")
    client.chat_postMessage(
        channel="nodehealth",
        blocks=slack_blocks,
    )
    print("posting file", result_file)
    # NOTE slackclient only accepts string based paths
    client.files_upload(channels="nodehealth", file=str(result_file), title="results file")

    if os.path.exists(save_file):
        os.remove(save_file)
    shutil.copyfile(result_file, save_file)


def clean_up_old_files(args):
    result_files = sorted(Path(args.path).glob("*-result.csv"))
    delete_files = result_files[:-args.keep_last]

    if len(delete_files) == 0:
        print("- no files marked for deletion")
        return

    print("- deleting the following old files:")
    for f in delete_files:
        print(f)
        f.unlink()


def files_equal(p1: Path, p2: Path) -> bool:
    return p1.exists() and p2.exists() and p1.read_bytes() == p2.read_bytes()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep-last", default=100, type=int, help="number of old files to keep")
    parser.add_argument("-p", "--path", default=".", help="path to store files")
    parser.add_argument("-c", "--checker", default="check_nodes.py", help="path to checker script")
    args = parser.parse_args()

    SLACK_TOKEN = os.environ["SLACK_TOKEN"]

    # run the checker and get the results saved to a file
    ts = int(time.time())
    result_file = Path(args.path, f"{ts}-result.csv")
    cmd = [
        "python3",
        args.checker,
        "--window",
        "2m",
        # "--ssh", # TODO provide ssh access
        "--uploads",
        "-o",
        str(result_file),
    ]
    print(f"- run checker: {cmd}")
    subprocess.check_output(cmd, timeout=120)

    print("- results:")
    print(result_file.read_text())

    # compare the results to see if there are any diffs
    report_file = Path(args.path, "report.csv")

    if report_file.exists():
        print("- previous report file exists")

    if files_equal(result_file, report_file):
        print("- results do NOT differ from last report, silent")
        return

    print("- results differ from last report")
    publish_results_to_slack(result_file, report_file, token=SLACK_TOKEN)

    clean_up_old_files(args)


if __name__ == "__main__":
    main()
