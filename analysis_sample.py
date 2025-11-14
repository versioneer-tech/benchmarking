import time
import psutil
import xarray as xr
import math
import random
from sklearn.cluster import KMeans
import os
import json
from urllib.parse import urlparse

URL = os.getenv(
    "URL",
    "https://s3.waw4-1.cloudferro.com/EarthCODE/OSCAssets/seasfire/seasfire_v0.4.zarr",
)
LARGE_JOB = os.getenv("LARGE_JOB", "").lower() in ("1", "true", "yes")
NUM_RUNS = int(os.getenv("NUM_RUNS", "5"))
RUN_ID = os.getenv("RUN_ID", "")
POD_NAME = os.getenv("POD_NAME", "")
WAIT = int(os.getenv("WAIT_TIME", "5")) if os.getenv("WAIT_TIME", "5").isdigit() else 5

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION = os.getenv("AWS_SESSION_TOKEN")
AWS_ENDPOINT = os.getenv("AWS_ENDPOINT")
AWS_REGION = os.getenv("AWS_REGION")


def _to_s3_url_and_endpoint(url: str):
    if url.startswith("s3://"):
        return url, AWS_ENDPOINT

    u = urlparse(url)
    path = u.path.lstrip("/")
    parts = path.split("/", 1)
    if len(parts) == 1:
        bucket, key = parts[0], ""
    else:
        bucket, key = parts

    s3_url = f"s3://{bucket}/{key}" if key else f"s3://{bucket}"
    endpoint = AWS_ENDPOINT or f"{u.scheme}://{u.netloc}"
    return s3_url, endpoint


def open_zarr_with_auto_auth(url):
    if AWS_ACCESS_KEY and AWS_SECRET_KEY:
        s3_url, endpoint_url = _to_s3_url_and_endpoint(url)

        client_kwargs = {}
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url
        if AWS_REGION:
            client_kwargs["region_name"] = AWS_REGION

        storage_options = {
            "key": AWS_ACCESS_KEY,
            "secret": AWS_SECRET_KEY,
        }
        if AWS_SESSION:
            storage_options["token"] = AWS_SESSION
        if client_kwargs:
            storage_options["client_kwargs"] = client_kwargs

        return xr.open_zarr(s3_url, chunks={}, storage_options=storage_options)

    return xr.open_zarr(url, chunks={})


# about 1 GB transferred
def larger_job():
    ds = open_zarr_with_auto_auth(URL)
    feat = xr.concat(
        [
            ds.cams_frpfire.isel(time=slice(-500, None)).mean("time"),
            ds.fwi_mean.isel(time=slice(-500, None)).mean("time"),
        ],
        "feature",
    ).isel(latitude=slice(None, None, 4), longitude=slice(None, None, 4)) \
     .stack(p=("latitude", "longitude")).transpose("p", "feature").fillna(0)

    return xr.DataArray(
        KMeans(4, n_init="auto").fit_predict(feat.compute().values),
        coords={"p": feat.p},
        dims="p",
    ).unstack("p")


# about 110 MB transferred
def small_job():
    ds = open_zarr_with_auto_auth(URL)
    frp = ds.cams_frpfire
    return frp.mean(("time", "latitude", "longitude")).compute()


def net_bytes():
    c = psutil.net_io_counters()
    return c.bytes_recv, c.bytes_sent


def wait_random():
    max_wait = WAIT * math.log(max(NUM_RUNS, 2))
    delay = random.uniform(0, max_wait)
    print(f"Start jitter: sleeping {delay:.1f} s (max {max_wait:.1f} s)")
    time.sleep(delay)


if __name__ == "__main__":
    wait_random()

    recv0, sent0 = net_bytes()
    t0 = time.perf_counter()

    job = larger_job if LARGE_JOB else small_job
    labels = job()

    wall = time.perf_counter() - t0
    recv1, sent1 = net_bytes()

    d_recv = recv1 - recv0
    d_sent = sent1 - sent0
    total = d_recv + d_sent

    print(f"Wall time:         {wall:.2f} s")
    print(f"Bytes received:    {d_recv/1e6:.2f} MB")
    print(f"Bytes sent:        {d_sent/1e6:.2f} MB")
    print(f"Total bytes:       {total/1e6:.2f} MB")

    throughput = total / 1e6 / wall if wall > 0 and total > 0 else 0.0
    if throughput > 0:
        print(f"Avg throughput:    {throughput:.2f} MB/s")

    metrics = {
        "run_id": RUN_ID,
        "pod_name": POD_NAME,
        "wall_seconds": wall,
        "bytes_received": d_recv,
        "bytes_sent": d_sent,
        "total_bytes": total,
        "avg_throughput_mb_s": throughput,
    }
    print("METRICS " + json.dumps(metrics))
