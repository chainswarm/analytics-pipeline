FROM nvidia/cuda:12.4.1-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive

# Install software-properties-common for add-apt-repository
RUN apt-get update && apt-get install -y --no-install-recommends \
    software-properties-common \
    && add-apt-repository ppa:deadsnakes/ppa -y \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
    python3.13 python3.13-venv python3.13-dev \
    build-essential curl \
    && rm -rf /var/lib/apt/lists/*

# Set Python 3.13 as default python3
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.13 1

# Bootstrap pip for Python 3.13 using get-pip.py
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.13

WORKDIR /app

COPY requirements.txt .

RUN python3.13 -m pip install --upgrade pip && \
    python3.13 -m pip install --no-cache-dir -r requirements.txt

ENV NX_CUGRAPH_AUTOCONFIG=1 \
    NX_CUGRAPH_VERBOSE=0 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

COPY packages/ ./packages/
COPY scripts/ ./scripts/

CMD ["python3.13", "-c", "print('Analytics pipeline image ready with Python 3.13. Specify service command in docker compose.')"]