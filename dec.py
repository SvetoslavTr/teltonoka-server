from datetime import datetime, timedelta

import pandas as pd

'''
    Class for decoding Codec 8 messages from Teltonica GPS receiver
    For detailed information please visit: https://wiki.teltonika-gps.com/view/Codec
    This class uses crawling method to read the data string
'''

class codec8Decoder:
    def __init__(self, data, allBeacons, collection):
        self.data = data
        self.allBeacons = allBeacons
        self.collection = collection

    def findProxymity(self):
        '''
        FIND MESSAGES BACK IN 2 HOURS COUNTAINS PROXIMITY
        '''
        timestampHourBack = datetime.now() - timedelta(minutes = 15)
        documentsAll = self.collection.find({
            '$and': [
                {'PROXIMITY': {'$eq':  50 } },
                {'timestamp': {'$gte': timestampHourBack}},
            ]
        },{'timestamp': 1})
        if len(list(documentsAll)) == 0:
            return True


    def propertyReader(self, readPosition, propertyLen):
        return int(self.data[readPosition:readPosition+propertyLen], 16), readPosition+propertyLen

    def propertyBReader(self, readBPosition, propertyBLen):
        return self.encodedBeaconData[readBPosition:readBPosition+propertyBLen], readBPosition+propertyBLen

    def decodeC8(self):
        recordToMongoDB = []
        wholeLen, readPosition = self.propertyReader(8,8)
        codec, readPosition = self.propertyReader(readPosition, 2)
        recordsCount, readPosition = self.propertyReader(readPosition, 2)

        # readPosition = 20
        for x in range(0,recordsCount):
            messageHeader = {}
            message = {}
            lenTimeStamp = 16
            timestamp,readPosition = self.propertyReader(readPosition, lenTimeStamp)
            lenPriority = 2
            priority, readPosition = self.propertyReader(readPosition, lenPriority)
            lenLongitude = 8
            longitude, readPosition = self.propertyReader(readPosition, lenLongitude)
            lenLatidude = 8
            latitude, readPosition = self.propertyReader(readPosition, lenLatidude)
            lenAltitude = 4
            altitude, readPosition = self.propertyReader(readPosition, lenAltitude)
            lenAngle = 4
            angle, readPosition = self.propertyReader(readPosition, lenAngle)
            lenSatelites = 2
            satelites, readPosition = self.propertyReader(readPosition, lenSatelites)
            lenSpeed = 4
            speed, readPosition = self.propertyReader(readPosition, lenSpeed)
            lenEventIOID = 2
            eventOIID, readPosition = self.propertyReader(readPosition, lenEventIOID)
            lenNTotalIDs = 2
            nTotalIDs, readPosition = self.propertyReader(readPosition, lenNTotalIDs)

            timestamp = datetime.fromtimestamp(int(str(timestamp)[:-3]))
            messageHeader = {
                'timestamp': timestamp,
                'priority': priority,
                'longitude': longitude,
                'latitude': latitude,
                'altitude': altitude,
                'angle': angle,
                'satelites': satelites,
                'speed': speed,
                'eventOIID': eventOIID,
                'nTotalIDs': nTotalIDs
                }

            ### START READ DIFFERENT IO IDS

            lenN1OneByteIO = 2
            N1OneByteIO, readPosition = self.propertyReader(readPosition, lenN1OneByteIO)
            if N1OneByteIO > 0:
                lenN1_IO_ID = 2
                lenN1_IO_Val = 2
                stopByte = readPosition + ( lenN1_IO_ID * N1OneByteIO ) + ( lenN1_IO_Val * N1OneByteIO )
                counter = 1

                while(stopByte != readPosition):
                    globals()[f"N1_IO_ID_{counter}"], readPosition = self.propertyReader(readPosition, lenN1_IO_ID)
                    globals()[f"N1_IO_Val_{counter}"], readPosition = self.propertyReader(readPosition, lenN1_IO_Val)
                    message[f"N1_IO_ID_{counter}"] = globals()[f"N1_IO_ID_{counter}"]
                    message[f"N1_IO_Val_{counter}"] = globals()[f"N1_IO_Val_{counter}"]
                    counter += 1


            lenN2TwoBytesIO = 2
            N2TwoBytesIO, readPosition = self.propertyReader(readPosition, lenN2TwoBytesIO)
            if N2TwoBytesIO > 0:
                lenN2_IO_ID = 2
                lenN2_IO_Val = 4
                stopByte = readPosition + ( N2TwoBytesIO * lenN2_IO_ID ) + ( N2TwoBytesIO * lenN2_IO_Val )
                counter = 1

                while(stopByte != readPosition):
                    globals()[f"N2_IO_ID_{counter}"], readPosition = self.propertyReader(readPosition, lenN2_IO_ID)
                    globals()[f"N2_IO_Val_{counter}"], readPosition = self.propertyReader(readPosition, lenN2_IO_Val)
                    message[f"N2_IO_ID_{counter}"] = globals()[f"N2_IO_ID_{counter}"]
                    message[f"N2_IO_Val_{counter}"] = globals()[f"N2_IO_Val_{counter}"]
                    print('N2 ##################### ',globals()[f"N2_IO_ID_{counter}"])
                    print('N2VAL ##################### ',globals()[f"N2_IO_Val_{counter}"])
                    if globals()[f"N2_IO_ID_{counter}"] == 6 and globals()[f"N2_IO_Val_{counter}"] > 3000:
                        print("################ - - - - - PROXIMITY UNDER 50 sm. - - - - - #######################")
                    counter += 1


            lenN4FourBytesIO = 2
            N4FourBytesIO, readPosition = self.propertyReader(readPosition, lenN4FourBytesIO)
            if N4FourBytesIO > 0:
                lenN4_IO_ID = 2
                lenN4_IO_Val = 8
                stopByte = readPosition + ( N4FourBytesIO * lenN4_IO_ID ) + ( N4FourBytesIO * lenN4_IO_Val )
                counter = 1
                while(stopByte != readPosition):
                    globals()[f"N4_IO_ID_{counter}"], readPosition = self.propertyReader(readPosition, lenN4_IO_ID)
                    globals()[f"N4_IO_Val_{counter}"], readPosition = self.propertyReader(readPosition, lenN4_IO_Val)
                    message[f"N4_IO_ID_{counter}"] = globals()[f"N4_IO_ID_{counter}"]
                    message[f"N4_IO_Val_{counter}"] = globals()[f"N4_IO_Val_{counter}"]
                    counter += 1

            lenN8EightBytesIO = 2
            N8EightBytesIO, readPosition = self.propertyReader(readPosition, lenN8EightBytesIO)
            if N8EightBytesIO > 0:
                lenN8_IO_ID = 2
                lenN8_IO_Val = 16
                stopByte = readPosition + ( N8EightBytesIO * lenN8_IO_ID ) + ( N8EightBytesIO * lenN8_IO_Val )
                while(stopByte != readPosition):
                    globals()[f"N8_IO_ID_{counter}"] , readPosition = self.propertyReader(readPosition, lenN8_IO_ID)
                    globals()[f"N8_IO_Val_{counter}"] , readPosition = self.propertyReader(readPosition, lenN8_IO_Val)
                    message[f"N8_IO_ID_{counter}"] = globals()[f"N8_IO_ID_{counter}"]
                    message[f"N8_IO_Val_{counter}"] = globals()[f"N8_IO_Val_{counter}"]
                    counter += 1

            recordToMongoDB.append(dict(messageHeader, **message))
        return recordToMongoDB


    def decodeC8E(self):
        recordToMongoDB = []
        wholeLen, readPosition = self.propertyReader(8,8)
        codec, readPosition = self.propertyReader(readPosition, 2)
        recordsCount, readPosition = self.propertyReader(readPosition, 2)
        print('RECORDS COUNT: ', recordsCount)
        # readPosition = 20
        for x in range(0,recordsCount):
            print("START ITERATE THE ", x, "RECORD AT", readPosition)
            messageHeader = {}
            message = {}
            lenTimeStamp = 16
            timestamp,readPosition = self.propertyReader(readPosition, lenTimeStamp)
            try:
                timestamp_ = datetime.fromtimestamp(int(str(timestamp)[:-3]))
            except ValueError:
                print("VALUE ERROR. THIS TIMESTAMP IS: ", timestamp)
                break
            print('TIMESTAMP', timestamp_)
            lenPriority = 2
            priority, readPosition = self.propertyReader(readPosition, lenPriority)
            print('PRORITY', priority)
            lenLongitude = 8
            longitude, readPosition = self.propertyReader(readPosition, lenLongitude)
            print('LONG', longitude)
            lenLatidude = 8
            latitude, readPosition = self.propertyReader(readPosition, lenLatidude)
            print('LAT',latitude)
            lenAltitude = 4
            altitude, readPosition = self.propertyReader(readPosition, lenAltitude)
            print("ALTITUDE", altitude)
            lenAngle = 4
            angle, readPosition = self.propertyReader(readPosition, lenAngle)
            print("ANGE", angle)
            lenSatelites = 2
            satelites, readPosition = self.propertyReader(readPosition, lenSatelites)
            print("SATELITES", satelites)
            lenSpeed = 4
            speed, readPosition = self.propertyReader(readPosition, lenSpeed)
            print("SPEED", speed)
            lenEventIOID = 4 # DIFF 8E
            eventOIID, readPosition = self.propertyReader(readPosition, lenEventIOID)
            print("EVEN OI ID", eventOIID)
            lenNTotalIDs = 4 # DIFF 8E
            nTotalIDs, readPosition = self.propertyReader(readPosition, lenNTotalIDs)
            print("COUNT TOTAL IDS", nTotalIDs)

            messageHeader = {
                'timestamp': timestamp_,
                'priority': priority,
                'longitude': longitude,
                'latitude': latitude,
                'altitude': altitude,
                'angle': angle,
                'satelites': satelites,
                'speed': speed,
                'eventOIID': eventOIID,
                'nTotalIDs': nTotalIDs
                }
            # print('HEADER: ', messageHeader)
            ### START READ DIFFERENT IO IDS

            lenN1OneByteIO = 4 # DIFF 8E
            N1OneByteIO, readPosition = self.propertyReader(readPosition, lenN1OneByteIO)
            if N1OneByteIO > 0:
                lenN1_IO_ID = 4 # DIFF 8E
                lenN1_IO_Val = 2
                stopByte = readPosition + ( lenN1_IO_ID * N1OneByteIO ) + ( lenN1_IO_Val * N1OneByteIO )
                counter = 1

                while(stopByte != readPosition):
                    globals()[f"N1_IO_ID_{counter}"], readPosition = self.propertyReader(readPosition, lenN1_IO_ID)
                    globals()[f"N1_IO_Val_{counter}"], readPosition = self.propertyReader(readPosition, lenN1_IO_Val)
                    message[f"N1_IO_ID_{counter}"] = globals()[f"N1_IO_ID_{counter}"]
                    message[f"N1_IO_Val_{counter}"] = globals()[f"N1_IO_Val_{counter}"]
                    counter += 1


            lenN2TwoBytesIO = 4 # DIFF 8E
            N2TwoBytesIO, readPosition = self.propertyReader(readPosition, lenN2TwoBytesIO)
            if N2TwoBytesIO > 0:
                lenN2_IO_ID = 4 # DIFF 8E
                lenN2_IO_Val = 4
                stopByte = readPosition + ( N2TwoBytesIO * lenN2_IO_ID ) + ( N2TwoBytesIO * lenN2_IO_Val )
                counter = 1

                while(stopByte != readPosition):
                    globals()[f"N2_IO_ID_{counter}"], readPosition = self.propertyReader(readPosition, lenN2_IO_ID)
                    globals()[f"N2_IO_Val_{counter}"], readPosition = self.propertyReader(readPosition, lenN2_IO_Val)
                    message[f"N2_IO_ID_{counter}"] = globals()[f"N2_IO_ID_{counter}"]
                    message[f"N2_IO_Val_{counter}"] = globals()[f"N2_IO_Val_{counter}"]

                    if globals()[f"N2_IO_ID_{counter}"] == 6 and globals()[f"N2_IO_Val_{counter}"] > 3000:
                        print("################ - - - - - PROXIMITY UNDER 50 sm. - - - - - #######################")
                        print('N2 ID  :',globals()[f"N2_IO_ID_{counter}"])
                        print('N2 VAL :',globals()[f"N2_IO_Val_{counter}"])

                        if codec8Decoder.findProxymity(self):
                            print('*************** THIS IS NEW PROXIMITY MESSAGE  FOR PERIOD ***********************')
                            message[f"PROXIMITY"] = 50
                        else:
                            message[f"PROXIMITY"] = 51

                    counter += 1


            lenN4FourBytesIO = 4 # DIFF 8E
            N4FourBytesIO, readPosition = self.propertyReader(readPosition, lenN4FourBytesIO)
            if N4FourBytesIO > 0:
                lenN4_IO_ID = 4 # DIFF 8E
                lenN4_IO_Val = 8
                stopByte = readPosition + ( N4FourBytesIO * lenN4_IO_ID ) + ( N4FourBytesIO * lenN4_IO_Val )
                counter = 1
                while(stopByte != readPosition):
                    globals()[f"N4_IO_ID_{counter}"], readPosition = self.propertyReader(readPosition, lenN4_IO_ID)
                    globals()[f"N4_IO_Val_{counter}"], readPosition = self.propertyReader(readPosition, lenN4_IO_Val)
                    message[f"N4_IO_ID_{counter}"] = globals()[f"N4_IO_ID_{counter}"]
                    message[f"N4_IO_Val_{counter}"] = globals()[f"N4_IO_Val_{counter}"]
                    counter += 1

            lenN8EightBytesIO = 4 # DIFF 8E
            N8EightBytesIO, readPosition = self.propertyReader(readPosition, lenN8EightBytesIO)
            if N8EightBytesIO > 0:
                lenN8_IO_ID = 4 # DIFF 8E
                lenN8_IO_Val = 16
                stopByte = readPosition + ( N8EightBytesIO * lenN8_IO_ID ) + ( N8EightBytesIO * lenN8_IO_Val )
                counter = 1
                while(stopByte != readPosition):
                    globals()[f"N8_IO_ID_{counter}"] , readPosition = self.propertyReader(readPosition, lenN8_IO_ID)
                    globals()[f"N8_IO_Val_{counter}"] , readPosition = self.propertyReader(readPosition, lenN8_IO_Val)
                    message[f"N8_IO_ID_{counter}"] = globals()[f"N8_IO_ID_{counter}"]
                    message[f"N8_IO_Val_{counter}"] = globals()[f"N8_IO_Val_{counter}"]
                    counter += 1

# 0001 number of NX X bytes IDs = 1

# 0181 NX 1  AVL ID = 385
# 002d Length of NX 45 bytes
# beacons
# 11  data part 1 byte
# 21f7826da64fa24e988024bc5b71e0893eea1ec334ba 22 bytes
# 21f7826da64fa24e988024bc5b71e0893e1596b6529d 22 bytes

            lenNXBytesIO = 4
            NXBytesIOCount, readPosition = self.propertyReader(readPosition, lenNXBytesIO) # number of NX bytesIO IDs (count)
            print('POSITION_', readPosition)
            for y in range(0,NXBytesIOCount):

                globals()[f"NX_IO_ID_{y}"], readPosition = self.propertyReader(readPosition, 4)
                message[f"NX_IO_ID_{y}"] = globals()[f"NX_IO_ID_{y}"]
                print('NX IO ID ', message[f"NX_IO_ID_{y}"])
                lenNX_IO_ID = 4
                lenNX_IO_Length = 4

                lenNX_Value, readPosition = self.propertyReader(readPosition, lenNX_IO_Length)
                if lenNX_Value < 10:
                    break
                lenNX_Value = lenNX_Value * 2
                print('NX IO LENGTH', lenNX_Value)

                stopByte = readPosition + lenNX_Value

                while(readPosition < stopByte):
                    print('cut possition', readPosition, 'LEN', lenNX_Value) # cut possition 104 LEN 46
                    print('BASI MAMATA', self.data[readPosition:readPosition+lenNX_Value])
                    self.encodedBeaconData  = self.data[readPosition+2:readPosition+lenNX_Value].decode('utf-8')

                    print('BeaconData' , self.encodedBeaconData)


                    stopBByte = readPosition + lenNX_Value # Calculate where to stop reading the beacons
                    readPosition = readPosition+2
                    print("STOP SEEKING BEACONS UNTIL WE REACH STOP B BYTE", stopBByte)
                    readBPosition = 0
                    counterBeacon = 1
                    while(readPosition < stopBByte):  # Loop for reading every beacon
                        print('BEACON LOOP INITIAL READ POSITION', readPosition)
                        if stopBByte == readPosition:
                            print("WHY DONT YOU STOP ?")

                        beaconModelFlags, readBPosition = self.propertyBReader(readBPosition, 2)
                        readPosition = readPosition+2
                        print('Beacon model flags: ', beaconModelFlags)
                        if beaconModelFlags == '21':
                            beaconName = 'iBeacon_rssi'
                            beaconLengths = {
                                'beaconUUID':32,
                                # 'beaconNamespace': 0,
                                # 'beaconInstanceID': 0,
                                'beaconMajor': 4,
                                'beaconMinor': 4,
                                'beaconRSSI': 2,
                                # 'beaconVoltage': 0,
                                # 'beaconTemperature': 0,
                                }

                        elif beaconModelFlags == '01':
                            beaconName = 'Eddystone_rssi'
                            beaconLengths = {
                                # 'beaconUUID':0,
                                'beaconNamespace': 20,
                                'beaconInstanceID': 12,
                                # 'beaconMajor': 0,
                                # 'beaconMinor': 0,
                                'beaconRSSI': 2,
                                # 'beaconVoltage': 0,
                                # 'beaconTemperature': 0,
                                }

                        elif beaconModelFlags == '07':
                            beaconName = 'Eddystone_voltage_tempeature_rssi'
                            beaconLengths = {
                                # 'beaconUUID':0,
                                'beaconNamespace': 20,
                                'beaconInstanceID': 12,
                                # 'beaconMajor': 0,
                                # 'beaconMinor': 0,
                                'beaconRSSI': 2,
                                'beaconVoltage': 4,
                                'beaconTemperature': 4,
                                }
                        message[f"beacon_{counterBeacon}_nameFlags"] = beaconName

                        singleBeaconMinor = singleBeaconMajor = singleBeaconRSSI = None
                        print("POSSITION BEFORE BEACON DATA ", readPosition) #  104
                        for k,beaconParamLen in beaconLengths.items():
                            beaconData, readBPosition =  self.propertyBReader(readBPosition, beaconParamLen)
                            readPosition = readPosition + beaconParamLen
                            print("CURRENT POSITION ------ > ", readPosition, 'BEACON DATA', beaconData)
                            if beaconData == '':
                                print('THIS SHIT IS EMPTY')
                                continue

                            if k == 'beaconMinor':
                                singleBeaconMinor = beaconData
                            if k == 'beaconMajor':
                                singleBeaconMajor = beaconData
                            if k == 'beaconRSSI':
                                singleBeaconRSSI = int(beaconData,16)



                        if all(v is not None for v in [singleBeaconMinor, singleBeaconMajor, singleBeaconRSSI]):
                            print("GOT IT - BEACON FOUND: singleBeaconMinor: ",singleBeaconMinor, "singleBeaconMajor ", singleBeaconMajor, "singleBeaconRSSI", singleBeaconRSSI )
                            beaconMAC = self.allBeacons.loc[
                                (self.allBeacons['major'] == int(singleBeaconMajor,16) ) &
                                (self.allBeacons['minor'] == int(singleBeaconMinor,16) )
                                ].mac.values[0]
                            message[f"beacon_{counterBeacon}_MAC"] = beaconMAC
                            message[f"beacon_{counterBeacon}_RSSI"] = singleBeaconRSSI
                            singleBeaconMinor = singleBeaconMajor = singleBeaconRSSI = None
                            counterBeacon += 1


            recordToMongoDB.append(dict(messageHeader, **message))
        return recordToMongoDB
