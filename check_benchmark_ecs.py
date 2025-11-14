#!/usr/bin/env python3
import argparse
import json
import statistics as stats
import sys
from typing import List, Dict
import boto3
from botocore.exceptions import BotoCoreError, ClientError


def fetch_cloudwatch_logs(
    log_group_name: str,
    region: str | None = None,
) -> List[Dict]:
    client = boto3.client("logs", region_name=region) if region else boto3.client("logs")

    events: List[Dict] = []
    next_token = None

    while True:
        kwargs = {
            "logGroupName": log_group_name,
            "filterPattern": "METRICS",
        }
        if next_token:
            kwargs["nextToken"] = next_token

        try:
            resp = client.filter_log_events(**kwargs)
        except (BotoCoreError, ClientError) as e:
            print(f"Error calling filter_log_events: {e}", file=sys.stderr)
            sys.exit(1)

        events.extend(resp.get("events", []))

        next_token = resp.get("nextToken")
        if not next_token:
            break
    
    return events


def parse_metrics(events: List[Dict], run_id: str | None = None) -> List[Dict]:
    rows: List[Dict] = []

    for ev in events:
        msg = ev.get("message", "").strip()
        if not msg.startswith("METRICS "):
            continue

        # Attach logStreamName as a stand-in for pod/task identity
        stream = ev.get("logStreamName", "<unknown-stream>")

        try:
            payload = msg.split(" ", 1)[1]
            data = json.loads(payload)
            data.setdefault("pod_name", stream)  # keep field name to reuse reporting code
        except Exception as exc:
            print(f"Failed to parse METRICS line: {msg!r} ({exc})", file=sys.stderr)
            continue

        if run_id is not None and data.get("run_id") != run_id:
            continue

        rows.append(data)

    return rows


def mean(xs: List[float]) -> float:
    return stats.mean(xs) if xs else 0.0


def print_report(run_id: str | None, records: List[Dict]) -> None:
    label = run_id if run_id is not None else "<any>"
    if not records:
        print(f"No METRICS lines found for run_id={label}")
        return

    print(f"=== Benchmark report for RUN_ID={label} ===")
    print(f"Tasks with metrics: {len(records)}")
    print()

    print("Per-task metrics:")
    for r in records:
        pod = r.get("pod_name", "<unknown>")
        wall = r.get("wall_seconds", 0.0)
        total = r.get("total_bytes", 0)
        thr = r.get("avg_throughput_mb_s", 0.0)
        print(
            f"  {pod:40s}  wall={wall:7.2f}s  "
            f"total={total/1e6:8.2f} MB  thr={thr:7.2f} MB/s"
        )

    walls = [r.get("wall_seconds", 0.0) for r in records]
    recv = [r.get("bytes_received", 0.0) for r in records]
    sent = [r.get("bytes_sent", 0.0) for r in records]
    total = [r.get("total_bytes", 0.0) for r in records]
    thrpt = [r.get("avg_throughput_mb_s", 0.0) for r in records]

    avg_wall = mean(walls)
    avg_recv = mean(recv)
    avg_sent = mean(sent)
    avg_total = mean(total)
    avg_thr = mean(thrpt)

    print()
    print("Averages over all tasks:")
    print(f"  Wall time:         {avg_wall:.2f} s")
    print(f"  Bytes received:    {avg_recv/1e6:.2f} MB")
    print(f"  Bytes sent:        {avg_sent/1e6:.2f} MB")
    print(f"  Total bytes:       {avg_total/1e6:.2f} MB")
    print(f"  Avg throughput:    {avg_thr:.2f} MB/s")


def main():
    parser = argparse.ArgumentParser(
        description="Collect and summarise benchmark METRICS from ECS Fargate CloudWatch logs."
    )
    parser.add_argument(
        "--run-id",
        required=False,
        help="Filter by run_id value embedded in METRICS JSON (e.g. run1). If omitted, include all.",
    )
    parser.add_argument(
        "--log-group-name",
        default="/ecs/analysis-sample",
        help="CloudWatch Logs group name (default: /ecs/analysis-sample).",
    )
    parser.add_argument(
        "--region",
        default=None,
        help="AWS region (default: use AWS CLI/boto3 default).",
    )
    args = parser.parse_args()

    events = fetch_cloudwatch_logs(args.log_group_name, args.region)
    records = parse_metrics(events, args.run_id)
    print_report(args.run_id, records)


if __name__ == "__main__":
    main()


"""
example usage: 

python check_benchmark_ecs.py \
  --run-id run1 \
  --log-group-name /ecs/analysis-sample \
  --region eu-west-2

"""