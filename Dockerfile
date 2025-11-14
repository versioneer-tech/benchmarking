FROM pangeo/pangeo-notebook:latest

WORKDIR /app

COPY analysis_sample.py .

ENV URL=https://s3.waw4-1.cloudferro.com/EarthCODE/OSCAssets/seasfire/seasfire_v0.4.zarr

CMD ["python", "analysis_sample.py"]
