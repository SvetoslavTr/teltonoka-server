#!/root/venv/bin/python
import socket
import sqlite3
import datetime


TCP_IP = '0.0.0.0'
TCP_PORT = 9999
BUFFER_SIZE = 1024
gpsdatafile = './gpsdata.txt'
# connection = sqlite3.connect('/root/watch/db.sqlite3')
# cursor = connection.cursor()

def datetimeGPS_Q50(dateGPS, timeGPS):
    year = '20'+dateGPS[4:]
    month = dateGPS[2:4]
    day = dateGPS[0:2]
    hour = timeGPS[0:2]
    minute = timeGPS[2:4]
    seconds = timeGPS[4:]
    newdate = year+'-'+month+'-'+day+' '+hour+':'+minute+':'+seconds
    return newdate



s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP,TCP_PORT))
s.listen(1)
f = open(gpsdatafile, "a")
while True:
    conn, addr = s.accept()
    print ("Connection from address: ", addr)
    while True:
        data = conn.recv(BUFFER_SIZE)
        data_ascii = data.decode("utf-8")
        if not data: break
        print ("Received data:", data_ascii)
        # f.write(data_ascii + '\n')
        # data_ascii = data_ascii.replace("[", "")
        # data_ascii = data_ascii.replace("]", "")
        # gpsvalues = data_ascii.split(',')
        # gpsvalues_ = []
        # if len(gpsvalues) > 10:
        #     for i in range(0,len(gpsvalues)):
        #         gpsvalues_.append(gpsvalues[i])
            # now_r = datetime.datetime.now()
            # now = datetimeGPS_Q50(gpsvalues[1], gpsvalues[2])
            # cursor.execute('INSERT INTO app_mappoints values (null,?,?,?,?,?)', (now,gpsvalues[3], gpsvalues_[4], gpsvalues_[6], now_r))
            # print(gpsvalues_[4], gpsvalues[6])
            # connection.commit()
    conn.close()
f.close()
