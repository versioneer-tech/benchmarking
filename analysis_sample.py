import time
import psutil
import xarray as xr
import math
import random
import dask.array as da
from sklearn.cluster import KMeans
import os
import json

# URL = "https://eoresults.esa.int/e/earthcode/seasfire_tests/seasfire_v0.4.zarr"
URL = os.getenv(
    "URL",
    "https://s3.waw4-1.cloudferro.com/EarthCODE/OSCAssets/seasfire/seasfire_v0.4.zarr",
)
LARGE_JOB = os.getenv("LARGE_JOB", "").lower() in ("1", "true", "yes")
NUM_RUNS = int(os.getenv("NUM_RUNS", "5"))
RUN_ID = os.getenv("RUN_ID", "")
POD_NAME = os.getenv("POD_NAME", "")
WAIT = int(os.getenv("WAIT_TIME", "5")) if os.getenv("WAIT_TIME", "5").isdigit() else 5


# about 1 GB transferred
def larger_job():
    ds = xr.open_zarr(URL, chunks={})
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
        coords={"p": feat.p}, dims="p",
    ).unstack("p")

# about 110 MB transferred
def small_job():
    ds = xr.open_zarr(URL, chunks={})
    frp = ds.cams_frpfire
    return frp.mean(("time", "latitude", "longitude")).compute()


def net_bytes():
    """Total bytes sent/recv on this host (all interfaces)."""
    c = psutil.net_io_counters()
    return c.bytes_recv, c.bytes_sent

def wait_random():
    max_wait = WAIT * math.log(max(NUM_RUNS, 2))
    delay = random.uniform(0, max_wait)
    print(f"Start jitter: sleeping {delay:.1f} s (max {max_wait:.1f} s)")
    time.sleep(delay)

if __name__ == "__main__":

    # add a random wait to avoid multiple containers starting at the same time and overwhelming the server
    # also simulate a real workshop environment
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

    throughput = 0.0
    if wall > 0 and total > 0:
        throughput = total / 1e6 / wall
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
