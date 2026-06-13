# teltonika-server

TCP server that listens for Teltonika FMB GPS tracker messages (Codec 8 and
Codec 8 Extended), decodes them, and stores raw documents in MongoDB.

A Django management command (`sync_teltonika`) then reads from MongoDB and
writes beacon sightings into the main MySQL database.
See `TELTONIKA_INTEGRATION.md` in the Django project for the full architecture.

---

## Files

| File | Purpose |
|------|---------|
| `gps.py` | TCP socket server, beacon lookup, MongoDB insert |
| `dec.py` | Codec 8 / 8E frame decoder |
| `beacons.csv` | Historical beacon export — **no longer used at runtime** (superseded by live MySQL lookup) |

---

## Dependencies

Install into the server's Python environment (`/root/venv`):

```bash
pip install pymongo pymysql
```

`pandas` is no longer required.

---

## Configuration (`gps.py`)

```python
MYSQL_CNF           = '/etc/unitradecluster-my.cnf'  # shared with Django
BEACON_REFRESH_SECS = 300   # reload beacon table from MySQL every 5 min
```

MongoDB connection is hardcoded at the top of `gps.py` (host, user, password).

---

## Starting the server

```bash
python gps.py
```

Or as a systemd service pointing to `/root/venv/bin/python gps.py`.

On startup the server:
1. Connects to MongoDB
2. Loads the beacon lookup table from MySQL (`core_beaconsdb`)
3. Listens on TCP port 9999 for device connections

---

## Beacon lookup

The server identifies BLE beacons by their iBeacon `major` / `minor` values.
It resolves them to a MAC address by querying the Django `core_beaconsdb` table
at startup and then every `BEACON_REFRESH_SECS` seconds, so beacons registered
in Django admin are picked up automatically within 5 minutes.

To add a new beacon: register it in Django admin (**Core → beaconsDB**) with
the correct MAC address, `major`, and `minor` values.

---

## Supported Teltonika codecs

| Codec | Method | BLE beacons |
|-------|--------|-------------|
| `08`  | `decodeC8()` | No (IO elements only) |
| `8E`  | `decodeC8E()` | Yes (iBeacon, Eddystone) |

---

## Kubernetes / k3s deployment

### Prerequisites

- k3s running (`k3s --version`)
- `kubectl` configured against the cluster
- `helm` v3 installed

### 1 — Pull the Helm chart dependencies

```bash
helm dependency update helm/teltonika
```

This fetches the Bitnami MongoDB subchart.

### 2 — Install

```bash
helm install teltonika helm/teltonika \
  --set mongodb.auth.rootPassword=<root-password> \
  --set mongodb.auth.password=<app-password> \
  --set credentials.mysqlUser=<mysql-user> \
  --set credentials.mysqlPass=<mysql-password>
```

Both `mongodb.auth.rootPassword` and `mongodb.auth.password` are **required** — the install aborts with a clear error if either is omitted.

`mongodb.auth.password` is the single source of truth for the app's MongoDB credentials; you do **not** need to set `credentials.mongoPass` separately.

### 3 — Verify

```bash
# All three pods should reach Running status
kubectl get pods -l app.kubernetes.io/instance=teltonika

# Check the external IP assigned by klipper-lb (k3s built-in)
kubectl get svc teltonika-server
```

Expected output:

```
NAME              TYPE           CLUSTER-IP     EXTERNAL-IP    PORT(S)          AGE
teltonika-server  LoadBalancer   10.43.x.x      192.168.x.x    9999:xxxxx/TCP   1m
```

Teltonika devices should point to `EXTERNAL-IP:9999`.

### 4 — Upgrade

After pushing a new image via the GitHub Actions pipeline:

```bash
helm upgrade teltonika helm/teltonika \
  --set mongodb.auth.rootPassword=<root-password> \
  --set mongodb.auth.password=<app-password> \
  --set credentials.mysqlUser=<mysql-user> \
  --set credentials.mysqlPass=<mysql-password>
```

Or pin a specific image tag:

```bash
helm upgrade teltonika helm/teltonika \
  ... \
  --set server.image.tag=sha-abc1234
```

### 5 — External MongoDB (optional)

To use an existing MongoDB instance instead of the in-cluster one:

```bash
helm install teltonika helm/teltonika \
  --set mongodb.enabled=false \
  --set mongodb.host=<mongo-hostname> \
  --set credentials.mongoUser=<user> \
  --set credentials.mongoPass=<password> \
  --set credentials.mysqlUser=<mysql-user> \
  --set credentials.mysqlPass=<mysql-password>
```

### Architecture

```
Teltonika devices
      │ TCP :9999
      ▼
┌─────────────────┐      ┌──────────┐
│ teltonika-server│─────▶│ MongoDB  │
│  (gps.py)       │      │ (Bitnami)│
└─────────────────┘      └────┬─────┘
                              │
┌─────────────────┐           │
│  sync-worker    │◀──────────┘
│ (sync_teltonika)│
└────────┬────────┘
         │
         ▼
      MySQL (external)
```

- **teltonika-server** — receives Codec 8/8E frames, writes raw documents to MongoDB
- **MongoDB** — in-cluster StatefulSet (Bitnami chart), data persisted on a `local-path` PV
- **sync-worker** — reads MongoDB, writes beacon sightings to Django's MySQL database
