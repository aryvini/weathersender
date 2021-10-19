import sys
from numpy.core.shape_base import block
from paho.mqtt.client import Client
import serial
from os import system,path
import time
import threading
import queue
from datetime import datetime,timedelta

import json

from src.davis_station import Station
from src.cloud_sender import connect_mongo, fetch_rain_data, send_mongo, send_wu,connect_mqtt




def open_serial(port):

    
    serialPort = serial.Serial(port=port, baudrate=115200, bytesize=8, timeout=2, stopbits=serial.STOPBITS_ONE)
        # Wait until there is data waiting in the serial buffer
    if not serialPort.is_open:
        print('Could not open serial')
        return False
    else: 
        return serialPort #return the serialPort object


def read_serial(port,q):

    while True:
        try:
            #read the serial port
            data = port.readline()
        except serial.SerialException:
            break
        
        ## if there's data, put in the queue object to be processed
        if len(data) > 0:
            
            q.put(data)



    # serial_data = []
    # if port.in_waiting > 0:
    #     serial_data.append(str(port.readline()))
    #     davis.parser(serial_data,saveraw=True)
    #     system('cls')
    #     davis.print_latest_data()

    return


def process_serial(queue):
    try:
        data=[]
        data.append(queue.get(block=False).decode('utf-8')) #block=False, otherwise an empty queuue will block exectuion until a item is added
        davis.parser(data,saveraw=config['save_raw_data'])
        
    except:
        return


def mqtt_upload(data:dict,client:Client):
    #select agg valeus of interest to save into db

    keys2store = ['windvKMH',
                    'windd',
                    'windgustKMH',
                    'tempC',
                    'rh',
                    'uv',
                    'solar',
                    'rain',
                    'rainsecs',
                    'rssi',
                    'packets',
                    'lostpackets',
                    'validrate',
                    ]

    data2send = {x: data[x] for x in data.keys() if x in keys2store}
    print(data2send)
    data2send['datetime'] = datetime.utcnow().isoformat()

    info = client.publish("weather",json.dumps(data2send))
    print(info.rc)
    return 



def mongo_upload(data_dict,mongo_collection):

    #select agg valeus of interest to save into db

    keys2store = ['windvKMH',
                    'windd',
                    'windgustKMH',
                    'tempC',
                    'rh',
                    'uv',
                    'solar',
                    'rain',
                    'rainsecs',
                    'rssi',
                    'packets',
                    'lostpackets',
                    'validrate',
                    ]

    data2send = {x: data_dict[x] for x in data_dict.keys() if x in keys2store}
    print(data2send)
    data2send['datetime'] = datetime.utcnow()
                
    if(send_mongo(data2send,mongo_collection).acknowledged):
        print("Stored in MongoDB")
        return

    
def wu_upload(data_dict):
    #Before upload data to weather underground, fetch rain data from mongo and update the hourly accumulated

    rain_data = fetch_rain_data(mongo_collection)
    data_dict = {**data_dict,**rain_data}

    send_wu(data_dict,config['stationId'],config['stationPwd'])

    return


#Function to round time
#copied from https://stackoverflow.com/questions/3463930/how-to-round-the-minute-of-a-datetime-object/10854034#10854034
def roundTime(dt=None, roundTo=60):
   """Round a datetime object to any time lapse in seconds
   dt : datetime.datetime object, default now.
   roundTo : Closest number of seconds to round to, default 1 minute.
   Author: Thierry Husson 2012 - Use it as you want but don't blame me.
   """
   if dt == None : dt = datetime.now()
   seconds = (dt.replace(tzinfo=None) - dt.min).seconds
   rounding = (seconds+roundTo/2) // roundTo * roundTo
   return dt + timedelta(0,rounding-seconds,-dt.microsecond)



CONFIG_FILE = 'config.json'

if __name__ == '__main__':


    #LOAD CONFIG FILE
    if path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
            print(config)

    else:
        print("Create the file {}.".format(CONFIG_FILE))
        exit(-12)

    mongo_collection = connect_mongo(config['connection_string'],config['dbname'],config['collection'])
    mqttclient = connect_mqtt(config['mqttserver'],config['mqttuser'],config['mqttpwd'])


    UPDATEINTERVAL = config['update_interval'] 

    
    ## Instantiate a davis vantage pro station
    davis = Station()
    Station.DEBUG = False

    #try to open serial
    try:
        port = open_serial(port=config['serial_port'])
    except serial.SerialException:
        print('Failed to open serial Port')
        exit(-1)


    #create a thread and queue to always listen to the serial port
    q = queue.Queue()
    #arguments to the read_serial function
    #port object and queue object
    t = threading.Thread(target=read_serial,args=(port,q),daemon=True) 
    #start the thread 
    print('Serial thread has started')
    t.start()


    nextUpdate = datetime.now() + timedelta(minutes=5)
    nextUpdate = roundTime(nextUpdate,roundTo=5*60)
    while True:
        #Main program here

        #put everything in a try statement
        try:
            #process queue
            #the processing must occurr before the aggregation, since they change the same object (Station davis)
            process_serial(q)

            davis.print_latest_data()
            time.sleep(2)
            system('clear')     


            if(datetime.now()>nextUpdate):
                aggdata = davis.get_aggregated_data()
                
                try:
                    mongo_upload(aggdata,mongo_collection)
                except Exception:
                    print('Error sending to mongo')

                try:
                    mqtt_upload(aggdata,mqttclient)
                except Exception:
                    print('Error sending to mqtt')

            
                try:
                    wu_upload(aggdata)
                    pass
                except Exception:
                    print('Error sending to W.U')


                nextUpdate = datetime.now() + timedelta(minutes=UPDATEINTERVAL)
                nextUpdate = roundTime(nextUpdate,roundTo=5*60)

            # ##zero accumulated daily rain for a new day
            # if((startdate.day - datetime.now().day) < 0):
            #     davis.clear_raindaily()
            #     startdate=datetime.now()
            #     pass


        except KeyboardInterrupt:
            port.close()
            break

    print('End of program')
    t.join()
    print('Goodbye')
