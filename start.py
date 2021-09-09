import sys
from numpy.core.shape_base import block
import serial
from os import system,path
import time
import threading
import queue
from datetime import datetime,timedelta

import json

from src.davis_station import Station
from src.cloud_sender import connect_mongo, send_mongo, send_wu




def open_serial(port='COM8'):

    
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
                    'raindaily',
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

    send_wu(data_dict,config['stationId'],config['stationPwd'])

    return



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

    nextUpdate = datetime.now() + timedelta(minutes=UPDATEINTERVAL)
    while True:
        #Main program here

        #put everything in a try statement
        try:
            #process queue
            #the processing must occurr before the aggregation, since they change the same object (Station davis)
            process_serial(q)

            davis.print_latest_data()
            time.sleep(2)
            system('cls')     


            if(datetime.now()>nextUpdate):
                aggdata = davis.get_aggregated_data()
                
                try:
                    mongo_upload(aggdata,mongo_collection)
                except Exception:
                    print('Error sending to mongo')

                

                try:
                    wu_upload(aggdata)
                    pass
                except Exception:
                    print('Error sending to W.U')


                nextUpdate = datetime.now() + timedelta(minutes=UPDATEINTERVAL)


        except KeyboardInterrupt:
            port.close()
            break

    print('End of program')
    t.join()
    print('Goodbye')
