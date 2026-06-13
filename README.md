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
