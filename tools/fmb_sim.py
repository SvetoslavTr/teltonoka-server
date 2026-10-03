"""Minimal Teltonika FMB simulator: IMEI handshake + one Codec 8 record per connection.

Sends a record with a GPS position and digital input DIN1 (IO ID 1), checks the ACK.

    python3 tools/fmb_sim.py <host> <port> <imei> <lat> <lon> <din1: 0|1>
    python3 tools/fmb_sim.py 195.230.12.98 9999 356307042441013 42.7117400 23.2417168 1

Use a test IMEI linked to a test container; DIN1=1 on a container near an active site
creates a real pickup request.
"""
import socket, struct, sys, time, binascii

def crc16_ibm(data):
    crc = 0
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ 0xA001 if crc & 1 else crc >> 1
    return crc

def codec8_packet(ts_ms, lat, lon, din1):
    gps = struct.pack('>iihhBH', int(round(lon * 1e7)), int(round(lat * 1e7)), 550, 0, 9, 0)
    io = bytes([1, 2,            # event IO = DIN1, 2 IO elements in total
                2, 1, din1, 239, 1,   # 2 x 1-byte IO: DIN1 (id 1), ignition (id 239)
                0, 0, 0])        # no 2-, 4-, 8-byte IOs
    avl = struct.pack('>QB', ts_ms, 0) + gps + io
    data = bytes([0x08, 1]) + avl + bytes([1])
    return b'\x00\x00\x00\x00' + struct.pack('>I', len(data)) + data + struct.pack('>I', crc16_ibm(data))

host, port, imei, lat, lon, din1 = sys.argv[1], int(sys.argv[2]), sys.argv[3], float(sys.argv[4]), float(sys.argv[5]), int(sys.argv[6])
s = socket.create_connection((host, port), timeout=10)
s.sendall(struct.pack('>H', len(imei)) + imei.encode())
hs = s.recv(16)
pkt = codec8_packet(int(time.time() * 1000), lat, lon, din1)
s.sendall(pkt)
ack = s.recv(64)
s.close()
print(f"handshake reply={hs!r}  sent {len(pkt)} bytes (DIN1={din1})  ack={ack!r} ({len(ack)} bytes)  "
      f"{'VALID 4-byte ACK' if ack == struct.pack('>I', 1) else 'NOT the 4-byte binary ACK a device expects'}")
