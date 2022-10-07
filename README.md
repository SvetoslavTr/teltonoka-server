# teltonoka-server
Server for listening and decoding for messages from Teltkonika

This is cappable to decode codecs 8 and 8 extended
The advanced codec supports beacon detection
The output is writen to MongoDB

Instalation:
1. Install Anaconda and activate your virtual environment.
2. Install the following python packages using pip
2.1 pip install pandas
2.2 pip install socket
3. Start server using one of the following method:
3.1. python gps.py
3.2. can be started as systemd service

