# --- Stage 1: base runtime ---
FROM python:3.11-slim AS base

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl git && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app/src

# Install minimal deps
COPY requirements.api.txt ./requirements.api.txt
RUN python -m pip install --upgrade pip && pip install --no-cache-dir -r requirements.api.txt

# --- Stage 2: cache Hugging Face model ---
FROM base AS model
# Pre-download model weights into the image layer
RUN python -c "from transformers import pipeline; pipeline('text-classification', model='unitary/toxic-bert', tokenizer='unitary/toxic-bert')"

# --- Stage 3: final image ---
FROM base AS final
WORKDIR /app
COPY --from=model /root/.cache/huggingface /root/.cache/huggingface

# Copy source code (API + libs)
COPY src ./src

# Ensure both /app and /app/src are on PYTHONPATH for src-layout + service imports
ENV PYTHONPATH=/app:/app/src

EXPOSE 8000
CMD ["uvicorn", "service.app:app", "--host", "0.0.0.0", "--port", "8000"]
