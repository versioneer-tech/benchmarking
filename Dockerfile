FROM python:3.13-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /app

COPY pyproject.toml .
COPY uv.lock .

RUN uv sync --frozen

COPY analysis_sample.py .

ENV PATH="/app/.venv/bin:${PATH}"

CMD ["python", "analysis_sample.py"]
