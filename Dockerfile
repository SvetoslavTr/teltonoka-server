# ── Stage 1: dependency builder ───────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Stage 2: minimal runtime image ───────────────────────────────────────────
FROM python:3.11-slim

# Non-root user for security
RUN useradd --no-create-home --shell /bin/false teltonika

WORKDIR /app

# Copy only installed packages from builder
COPY --from=builder /install /usr/local

# Copy application source
COPY gps.py dec.py ./

USER teltonika

# TCP port the Teltonika devices connect to
EXPOSE 9999

# Env-var defaults (all overridable at runtime / via Helm values)
ENV MONGO_HOST=mongodb \
    MONGO_PORT=27017 \
    MONGO_DB=trackersData \
    MYSQL_HOST=mysql \
    MYSQL_PORT=3306 \
    MYSQL_DB=unitradecluster \
    TCP_PORT=9999 \
    BEACON_REFRESH_SECS=300

CMD ["python", "-u", "gps.py"]
