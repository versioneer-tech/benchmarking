FROM pangeo/pangeo-notebook:latest

WORKDIR /app

COPY analysis_sample.py .

CMD ["python", "analysis_sample.py"]
