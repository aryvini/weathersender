## Classes and functions to decode raw string comming from an arduino with VPtools ISSx sketch

from re import compile,findall
import pandas as pd
import numpy as np
from datetime import datetime
from prettytable import PrettyTable
from math import sin,cos,pi,atan2,degrees


class Station():

    DEBUG = False

    RAINBUCKET = 0.2

    RAW_PARAMS = ['windv','windd','windgust','gustref','temp','rh','uv','solar','rain','rainsecs',
                    'rssi','packets','lostpackets']

    REGS = {'windv':'(?:windv:)(.?\d{1,3})',          #Windv: mph
            'windd':'(?:windd:)(.?\d{1,3})',           #windd degs
            'windgust':'(?:windgust:)(.?\d{1,3})',     #?
            'gustref':'(?:gustref:)(.?\d{1,3})',       #?
            'temp':'(?:temp:)(.?\d{1,3})',             # fahrenheit
            'rh':'(?:rh:)(.?\d{1,3})',                 # %
            'uv':'(?:uv:)(.?\d{1,3})',                 # index
            'solar':'(?:solar:)(.?\d{1,3})',           #W/m²
            'rain':'(?:rain:)(.?\d{1,3})',             #??
            'rainsecs':'(?:rainsecs:)(.?\d{1,3})',     #??
            'rssi':'(?:rssi:)(.?\d{1,3})',             #db
            'packets':'(?:packets:)(\d{1,})',
            'lostpackets':'(?:packets:)(?:\d{1,})(?:\/)(\d{1,})',
        }
    

    def __init__(self):

        ##dict to store incomming data
        ##{parameter,pd.Series}
        self.raw_series = {}
        for par in Station.RAW_PARAMS:
            self.raw_series[par] = pd.Series(name=par,dtype='float64')


        #Aggregated and Calculated values
        self.agg_params={}
        self.agg_params = {
            'windvMPH': 0,
            'windvKMH':0,
            'windd':0,
            'windgustMPH':0,
            'windgustKMH':0,
            'gustref':0,
            'tempF':0,
            'tempC':0,
            'rh':0,
            'uv':0,
            'solar':0,
            'rain':0,
            #'raindaily':0,
            'rainsecs':0,
            'rssi':0,
            'packets':0,
            'lostpackets':0,
            'validrate':0,
        }
        

        pass

    def parser(self,rawdata,saveraw=True):
        #rawdata is a list with lines
        if Station.DEBUG:
            print(f'Data: {rawdata}')
        if saveraw:
            self.__save_raw_data(rawdata)


        for line in rawdata:
            for par,reg in zip(Station.REGS,Station.REGS.values()):
                
                pattern = compile(reg)
                matches = pattern.findall(line)

                if len(matches) == 1:
                    
                    value = pd.to_numeric(matches[0])
                    self.raw_series[par] = self.raw_series[par].append(pd.Series(data=value))

                    if Station.DEBUG:
                        print(f'{par}:{value}')

        pass

    def __aggregate_data(self):
        series = self.raw_series
        agg = self.agg_params

        self.__process_wind()

        self.__process_rain()

        agg['tempF'] = round(series['temp'].mean()) if len(series['temp'])>0 else np.nan
        if not pd.isna(agg['tempF']):
            agg['tempC'] = round((agg['tempF']-32)/1.8,2)
        else:
            agg['tempC'] = np.nan

        
        
        agg['uv'] = round(series['uv'].mean()) if len(series['uv'])>0 else np.nan
        agg['rh'] = round(series['rh'].mean()) if len(series['rh'])>0 else np.nan
        agg['solar'] = round(series['solar'].mean()) if len(series['solar'])>0 else np.nan
        agg['rssi'] = round(series['rssi'].mode()[0]) if len(series['rssi'])>0 else np.nan
        agg['packets'] = round(series['packets'].tolist()[-1]) if len(series['packets'])>0 else np.nan
        agg['lostpackets'] = round(series['lostpackets'].tolist()[-1]) if len(series['lostpackets'])>0 else np.nan
        
        try:
            agg['validrate'] = round((agg['packets']/(agg['packets']+agg['lostpackets']))*100,2)
        except:
            agg['validrate'] = np.nan

        return 

    
    def get_aggregated_data(self):
        '''
        Returns a dict with aggregated values.
        Clear series and values for new cycle.

        '''


        self.__aggregate_data()

        data = self.agg_params.copy()

        self.__clear()

        return data




    def __process_rain(self):
        s = self.raw_series
        agg = self.agg_params
        diff=0
        rainList = s['rain'].tolist()
        try:
            if self.__rain_pairity_check in rainList:
                print(f'Rain pairity in list: {self.__rain_pairity_check}')

                pass
            else:
                print('Rain pairity not in list, inserting...')
                rainList.insert(0,self.__rain_pairity_check)
        except:
            print('Rain pairity not sync yet')
            pass

        if len(rainList) >= 2:
            first = rainList[0]
            print(f'First rain tick: {first}')
            last = rainList[-1]
            self.__rain_pairity_check = last
            print(f'Pairity sync: {self.__rain_pairity_check}')
            print(f'Last rain tick: {last}')
            diff=(last-first)
            print(f'Diff: {diff}')

            if diff < 0:
                diff=diff+128
            else:
                pass

            print(f'Processed rain ticks: {diff}')

            
            agg['rain'] = round(diff*Station.RAINBUCKET,2) if len(s['rain'])>0 else np.nan
            agg['rainsecs'] = round(s['rainsecs'].tolist()[-1]) if len(s['rainsecs'])>0 else np.nan

            # agg['raindaily'] = round(agg['raindaily']+agg['rain'],2)

    
        else:
            print('No rain data has been reported')
            return


    def __process_wind(self):
        series = self.raw_series
        agg=self.agg_params

        ###########
        #AVERAGING WIND DIRECTION AND SPEEDS

        ##source: 
        #https://www.researchgate.net/publication/262766424_Technical_note_Averaging_wind_speeds_and_directions?enrichId=rgreq-3f8efd7fe436350ec11a79d36ca6d644-XXX&enrichSource=Y292ZXJQYWdlOzI2Mjc2NjQyNDtBUzoyMDMwMTYxMTc0NjA5OTRAMTQyNTQxNDIyMzAwNw%3D%3D&el=1_x_3&_esc=publicationCoverPdf
        
        if((len(series['windv'])>1) & (len(series['windd'])>1)):
            #create a df with speed and direction with both series
            ##windv in MPH and windd degress


            df = pd.concat([series['windv'],series['windd']],axis=1)
            df.columns = ['windv','windd']

            #Decomponse the vector in x,y direction (u,v)
            df['u'] = df.apply(lambda x: -x['windv']*sin(2*pi * x['windd']/360),axis=1)
            df['v'] = df.apply(lambda x: -x['windv']*cos(2*pi * x['windd']/360),axis=1)
            
            #averaged values
            # vel_vector_avg = sqrt(df['u'].mean()**2 + df['v'].mean()**2)
            dir_vector_avg = (atan2(df['u'].mean(),df['v'].mean()) * 360/2/pi) + 180
            vel_scalar_avg = df['windv'].mean()

            #set to dict
            agg['windvMPH'] = round(vel_scalar_avg,2)
            agg['windvKMH'] = round(vel_scalar_avg*1.6,2)
            agg['windd'] = round(dir_vector_avg)
            
            pass
        
        else:
            agg['windvMPH'] = np.nan
            agg['windvKMH'] = np.nan
            agg['windd'] = np.nan

            pass

        ##WIND GUSTS

        ##save the max wind gust in each aggregation time

        if(len(series['windgust'])>=1):
            agg['windgustMPH'] = series['windgust'].max()
            agg['windgustKMH'] = round(series['windgust'].max() * 1.6,2)

        else:
            agg['windgustMPH'] = np.nan
            agg['windgustKMH'] = np.nan



        return

    def __clear(self):
        
        #to clear series and aggregate values, call __init__()
        self.__init__()

        return

        
    def print_latest_data(self):
        s = self.raw_series
        #dict to hold headers and correspondent variables
        latest = {}
        samples = {}
        table = PrettyTable()
        for par,series in zip(s.keys(),s.values()):
            try:
                latest[par] = series.tolist()[-1]
                samples[par] = len(series)
            except:
                latest[par] = "--"
                samples[par] = "--"

        
        table.add_column('par',['-','n'])

        for header,value,samples in zip(latest.keys(),latest.values(),samples.values()):
            # print(header)
            # print(value)
            table.add_column(str(header),[value,samples])
        
        try:
            validrate = round((latest['packets']/(latest['packets']+latest['lostpackets']))*100,2)
            # lostrate = latest['lostpackets']/latest['packets']*100
        except:
            validrate = 0

        # raindaily = self.agg_params['raindaily']
        # table.add_column('DailyRain',[f'{raindaily:.2f}','--'])
        table.add_column('% valid',[f'{validrate:.2f}','--'])
            
        return print(table)


    def clear_raindaily(self):
        self.agg_params['raindaily'] = 0
        return


    
    def __save_raw_data(self,rawdata):

        with open(f'rawdata/rawdata.txt','a') as f:
            f.write(datetime.isoformat(datetime.utcnow()))
            f.write(',')
            f.write(str(rawdata))
            f.write('\n')
            f.close()
















