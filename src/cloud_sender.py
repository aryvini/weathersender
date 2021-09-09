from pandas.core.dtypes.missing import isna
from pymongo import MongoClient
import pandas as pd
import requests
from requests.api import request


def connect_mongo(string,db_name,collection_name):
    client = MongoClient(string)
    dbname = client[db_name]
    collection = dbname[collection_name]

    return collection

def send_mongo(object,collection):
    return collection.insert_one(object)



def send_wu(object,stationId,stationPwd):
    WUurl = "https://weatherstation.wunderground.com/weatherstation/updateweatherstation.php?"
    creds = "ID="+stationId+"&PASSWORD="+ stationPwd
    dateStr = "&dateutc=now"
    actionStr = "&action=updateraw"

    reqStr = WUurl+creds+dateStr+actionStr
    send=0
    s = object

    if not (pd.isna(s['windvMPH'])):
        reqStr = reqStr+'&windspeedmph='+ f'{s["windvMPH"]:.2f}'
        send=send+1
    
    if not (pd.isna(s["windd"])):
        reqStr = reqStr+'&winddir='+f'{s["windd"]}'
        send=send+1

    if not(pd.isna(s["windgustMPH"])):
        reqStr = reqStr+'&windgustmph='+f'{s["windgustMPH"]}'
        send=send+1

    if not(pd.isna(s["tempF"])):
        reqStr = reqStr + '&tempf='+ f'{s["tempF"]:.2f}'
        send=send+1

    if not(pd.isna(s["uv"])):
        reqStr = reqStr + '&UV=' + f'{s["uv"]:.2f}'
        send=send+1

    if not(pd.isna(s["rh"])):
        reqStr = reqStr +"&humidity=" + f'{s["rh"]:.2f}'
        send=send+1

    if not(pd.isna(s["solar"])):
        reqStr = reqStr + '&solarradiation='+ f'{s["solar"]:.2f}'
        send=send+1

    if not(pd.isna(s["raindaily"])):
        raindailyin = round(s["raindaily"] * 0.039,2)
        reqStr = reqStr + '&dailyrainin='+ f'{raindailyin:.2f}'
        send = send+1

    if send == 0:
        return False

    
    r = requests.get(reqStr)
    print("Received " + str(r.status_code) + " " + str(r.text))
    if r.status_code == 200:
        print('Data send to Weather Underground')
        return True
    else:
        print('Data not sent to Weather Underground')
        return False


