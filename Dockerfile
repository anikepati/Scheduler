# Multi-stage build — keeps final image lean
FROM python:3.12-slim AS builder

WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


FROM python:3.12-slim

# OCP runs containers as non-root by default
# Use a high UID to satisfy OpenShift's arbitrary UID policy
RUN groupadd -g 1001 scheduler && \
    useradd -u 1001 -g scheduler -s /bin/bash -m scheduler

WORKDIR /app

COPY --from=builder /install /usr/local
COPY *.py ./

# OCP injects POD_NAME via downward API — see deployment.yaml
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LOG_LEVEL=INFO

USER 1001

EXPOSE 8080

ENTRYPOINT ["python", "main.py"]
