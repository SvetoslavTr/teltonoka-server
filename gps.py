import socket

import threading
import binascii
import pandas as pd

from dec import codec8Decoder
from pymongo import MongoClient, errors
client = MongoClient('localhost',
                    username='sdm',
                    password='k3r34',
                    authSource='admin',
                    authMechanism='SCRAM-SHA-256')
db = client['trackersData']
collection = db['data']
collection.create_index([('timestamp',1),('imei',1)], unique=True, dropDups=1)

port = 9999
allBeacons = pd.read_csv('beacons.csv',usecols=['mac', 'major', 'minor'])

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(('', port))


def decodethis(data, imei):

    if (len(data) > 0):
        print("[ DATA PROCESSING ]")
        codec = data[16:18].decode('utf-8')
        if codec == '08':
            lenght = int(data[8:16], 16)
            record = int(data[18:20], 16)
            timestamp = int(data[20:36], 16)
            priority = int(data[36:38], 16)
            # lon = int(data[38:46], 16)
            lon = data[38:46].decode('utf-8')
            lat = int(data[46:54], 16)
            alt = int(data[54:58], 16)
            angle = int(data[58:62], 16)
            sats = int(data[62:64], 16) #maybe
            speed = int(data[64:68], 16)
            print(f'[ DATA LENGTH {len(data[38:46])}')
            print(f"[ N RECORDS:  {str(record)} ] \n[ TIMESTAMP: {str(timestamp)} ] \n[ LAT,LON: {str(lat)} {str(lon)} ] \n[ ALTITUDE: {str(alt)} ]\n[ SATS:  {str(sats)} ]\n[ SPEED: {str(speed)} ] \n")
            sux = codec8Decoder(data,'',collection).decodeC8()
            imei = imei.strip('\x00').strip('\x0f')
            for x in sux:
                x['imei'] = imei
                try:
                    insertedDocument = collection.insert_one(x).inserted_id
                    print('INSERTED DOCUMENT: ', insertedDocument)
                except errors.DuplicateKeyError:
                    # print('Sorry, refusing to insert. This is fuckin\' Duplicate entry')
                    pass

        elif codec == '8e':
            sux = codec8Decoder(data,allBeacons,collection).decodeC8E()
            imei = imei.strip('\x00').strip('\x0f')
            for x in sux:
                x['imei'] = imei
                try:
                    insertedDocument = collection.insert_one(x).inserted_id
                    print('INSERTED DOCUMENT: ', insertedDocument)
                except errors.DuplicateKeyError:
                    # print('Sorry, refusing to insert. This is fuckin\' Duplicate entry')
                    pass

    return b"000000"+data[18:20]

def handle_client(conn, addr):
    print(f"[ NEW CONNECTION:  {addr} connected. ]")
    connected = True
    laterSendRecord = b''
    while connected:
        try:
            imei = conn.recv(1024)
        except  ConnectionResetError:
            print("==> ConnectionResetError")
            pass
        except socket.timeout:
            print("==> Timeout")
            pass

        if len(imei) == 0:
            conn.close()
            break
        imei_ = binascii.hexlify(imei)
        imeiLen = int(imei_[0:4], 16)
        if imeiLen != 15:
            conn.close()
            break
        else:
            print('[ IMEI  '+imei.decode('utf-8')+']')

        try:
            message = '\x01'
            message = message.encode('utf-8')
            conn.send(message)
        except:
            print("[ ERROR ] Error sending reply. Maybe it's not our device")


        try:
            data = conn.recv(4096)
            if len(data) == 0:
                conn.sendall(laterSendRecord)
                conn.close()
                connected = False
                break

            received = binascii.hexlify(data)
            print(received)

            record = decodethis(received, imei.decode('utf-8'))
            laterSendRecord = record
            print(f'[ RESPONSE TO MODULE: {record}]')
            conn.sendall(record)
            print(f'[ ------------- END OF COMMUNICATION --------------------- ]')
            print(f'\n\n\n')
        except socket.error:
            print("[ ERROR ] Error Occured.")
            break


    conn.close()



def start():
    s.listen()
    print(f'[ SERVER LISTENING ... ]')

    while True:
        conn, addr = s.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()
        print(f"[ ACTIVE CONNECTIONS: {threading.activeCount() - 1} ]")
print(f'[ SERVER STARTING  ... ]')
try:
    start()
except KeyboardInterrupt:
    s.shutdown(1)
    s.close()
    exit('\r[ ----------- EXIT FROM HELL ------------ ]')
start()
