# Benchmarking

> [!NOTE]  
> Based on fork of https://github.com/sunnydean/https---github.com-sunnydean-eo-cloud-data-benchmark/

Submit Kubernetes benchmark jobs using:

    ./submit-job.sh <name> [flags...]

## Basic Usage

You must already be connected to your cluster.

e.g.

```
  ./submit-job.sh analysis-sample
```

```
  ./submit-job.sh analysis-sample \
  --completions 1 \
  --parallelism 1 \
  --num-runs 1 \
  --large-job false
```

```
  ./submit-job.sh analysis-sample \
  --completions 10 \
  --parallelism 10 \
  --num-runs 5 \
  --large-job true \
  --aws-access-key ... \
  --aws-secret-key ... \
  --aws-endpoint ... \
  --aws-region ...
```