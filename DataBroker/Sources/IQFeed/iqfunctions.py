import logging
import socket
import datetime
import time
import re
import pytz
from ratelimiter import RateLimiter
from DataBroker.Sources.IQFeed.database import databaseHandler
import DataBroker.Sources.IQFeed.options_generator as og
import copy
import pandas as pd
import pandas.io.sql as sqlio

logger = logging.getLogger(__name__)
class iqFunctions:
    def __init__(self,postgresParams={},debug=False,insert=False,iqHost="10.6.47.58",iqPort=9101,tablesToInsert=[]):
        '''
        Class to perform functions on IQFeed API.
        postgresParams -> (dict) Dict with keys host, port, database, user, \
                            password for Postgres database
        debug -> (boolean) Whether to record debug logs
        insert -> (boolean) whether to insert into database
        iqHost -> (str) Host address of IQFeed
        iqPort -> (str) Port of IQFeed Socket
        tablesToInsert -> (list) List of string for each table name to insert
        '''
        # Define server host, port and symbols to download
        self.tableToInsert = tablesToInsert
        host = iqHost  # Historical data socket host
        port = iqPort  # Historical data socket port
        # Open a streaming socket to the IQFeed server locally
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))
        self.insert = insert
        self.data =  {
            "iqpricehistory_min": [],
            "iqpricehistory_daily": []
        }
        self.nyt = pytz.timezone('America/New_York')
        if logger != None:
            self.log = logger
        else:
            self.log = logging.getLogger()
            self.log.setLevel(logging.DEBUG)
            self.fh = logging.FileHandler(filename=f'./logs/output_{datetime.datetime.now(tz=self.nyt).date()}.txt')
            if debug:
                self.fh.setLevel(logging.DEBUG)
            else:
                self.fh.setLevel(logging.INFO)
            self.log.addHandler(self.fh)
        self.postgres = postgresParams
        # Connect to Postgres
        self.startTime = time.time() 
        self.log.info(f'')
        self.log.info(f'')
        self.log.info(f'')
        self.log.info(f'IqFeed Databroker')
        self.log.info(f'Starting Run at: {self.startTime}')
        self.db = databaseHandler(self.postgres)

    def read_historical_data_socket(self,recv_buffer=4096):
        '''
        Read the information from the socket, in a buffered
        fashion, receiving only 4096 bytes at a time.

        Parameters:
        recv_buffer - Amount in bytes to receive per read
        '''
        buffer = ""
        data = ""
        while True:
            data = self.sock.recv(recv_buffer)
            
            buffer += bytes.decode(data)
            # Check if the end message string arrives
            if "!ENDMSG!" in buffer:
                break
    
        # Remove the end message string
        buffer = buffer[:-12]
        return buffer

    def makeRequest(self,sym,api,frequency=None):
        '''
        Request data for a specific asset.
        sym -> (str) Ticker security
        api -> (str) Which API to call: 'price'
        frequency -> (str) For price API frequency of data: 'minute', 'daily'
        '''
        rate_limiter = RateLimiter(max_calls=2,period=1,callback=self.execute_mogrify)
        with rate_limiter:
            if (api == 'price'):
                if(bool(frequency)):
                    self.log.info("Downloading symbol: %s..." % sym)
                    # Construct the message needed by IQFeed to retrieve data
                    # Format:
                    # CMD,SYM,[options]\n.
                    # Example Historical Data:
                    # Historical,Ticker,Rate (Seconds),Start,End (Default:Yesterday),empty,beginning time filter: HHmmSS,ending time filter: HHmmSS,old or new: 0 or 1,[empty,queue data points per second
                    if frequency.lower()=="minute":
                        frequency = "minute"
                        frequencyStr = str(60)
                        message = "HIT,%s,%s,20200101 073000,,,073000,160000,1\n" % (sym, frequencyStr)
                    elif frequency.lower()=="daily":
                        frequency = "daily"
                        message = "HDT,%s,20180101,,,1\n" % sym
                    else:
                        self.log.error("No frequency stated")
                        return

                    # Send the historical data request
                    # message and buffer the data
                    self.sock.sendall(message.encode('utf-8'))
                    data = self.read_historical_data_socket()

                    # Remove all the endlines and line-ending
                    # comma delimiter from each record
                    datals = "".join(data.split("\r"))
                    datals = datals.replace(",\n","\n%s," % sym)
                    datals = f"{sym}," + datals[:-1]
                    if frequency == "daily":
                        datals = re.sub('\s\d*:\d*:\d*','',datals)
                    datals = datals.split("\n")
                    dataList = [data.split(",") for data in datals]
                     
                    if (frequency in self.data.keys()):
                        self.data[frequency].extend(dataList)
                    else:
                        self.data[frequency] = dataList
        if self.insert:
            self.execute_mogrify(None)
        return

    def makeRequests(self,syms,api,frequency=None,beginDate=None,beginDateTable=pd.DataFrame,buffer=10):
        '''
        Request data for multiple assets.
        syms -> (list) List of tickers
        api -> (str) Which API to call: 'price'
        frequency -> (str) For price API frequency of data: 'minute', 'daily'
        beginDate -> (str) Datetime to begin data lookup 'YYYYMMdd HHMMSS'
        buffer -> (int) Number of searches before insert
        '''
        rate_limiter = RateLimiter(max_calls=20,period=1,callback=self.execute_mogrify)
        i = 0
        if frequency == None:
            self.log.error("No frequency stated")
            return
        else:
            frequency = frequency.lower()
            if frequency not in ['iqpricehistory_min', 'iqpricehistory_daily']:
                self.log.error("Invalid frequency stated")
                return
        for sym in syms:
            # Insert into database every certain number of searches
            if i < buffer:
                with rate_limiter:
                    if (api == 'price'):
                        if(bool(frequency)):
                            self.log.info("Downloading symbol: %s..." % sym)
                            # Construct the message needed by IQFeed to retrieve data
                            # Format:
                            # CMD,SYM,[options]\n.
                            # Example Historical Data:
                            # Historical,Ticker,Rate (Seconds),Start,End (Default:Yesterday),empty,beginning time filter: HHmmSS,ending time filter: HHmmSS,old or new: 0 or 1,[empty,queue data points per second
                            endDate = og.datetimeField(datetime.datetime.now(self.nyt))
                            if frequency.lower()=="iqpricehistory_min":
                                if sym not in beginDateTable.index:
                                    frequencyStr = str(60)
                                    message = "HIT,%s,%s,%s,%s,,073000,160000,1\n" % (sym, frequencyStr,beginDate,endDate)
                                else:
                                    frequencyStr = str(60)
                                    message = "HIT,%s,%s,%s,%s,,073000,160000,1\n" % (sym, frequencyStr,beginDateTable.loc[sym]['Date'],endDate)
                            elif frequency.lower()=="iqpricehistory_daily":
                                if sym not in beginDateTable.index:
                                    message = "HDT,%s,%s,%s,,1\n" % (sym,beginDate,endDate)
                                else:
                                    message = "HDT,%s,%s,%s,,1\n" % (sym,beginDateTable.loc[sym]['Date'],endDate)
                            else:
                                self.log.error("Invalid frequency stated")
                                return

                            # Send the historical data request
                            # message and buffer the data
                            self.sock.sendall(message.encode('utf-8'))
                            data = self.read_historical_data_socket()

                            # Remove all the endlines and line-ending
                            # comma delimiter from each record
                            #print(data)
                            datals = "".join(data.split("\r"))
                            datals = datals.replace(",\n","\n%s," % sym)
                            datals = f"{sym}," + datals[:-1]
                            if frequency == "daily":
                                datals = re.sub('\s\d*:\d*:\d*','',datals)
                            datals = datals.split("\n")
                            dataList = [data.split(",") for data in datals]
                            
                            if (frequency in self.data.keys()):
                                self.data[frequency].extend(dataList)
                            else:
                                self.data[frequency] = dataList
                i += 1
            else:
                i = 0
                self.execute_mogrify(None)
        if self.insert:
            self.execute_mogrify(None)
        return

    def exit(self):
        '''
        Function to exit class.
        '''
        if self.insert:
            self.execute_mogrify(None)
        self.endTime = time.time() 
        self.log.info("Closing socket for IQ Feed")
        self.log.info(f'Ending Run at: {self.endTime}')
        self.log.info(f'Runtime: {self.endTime - self.startTime}')
        self.db.exit()
        self.sock.close
    
    def execute_mogrify(self,until):
        '''
        Wrapper function for execute_mogrify to make it a callback function for rate_limiter.
        '''
        data = {}
        data = copy.deepcopy(self.data)
        for key in self.data:
            self.data[key].clear()
            self.log.info("mogrify: " + key)
            self.log.info('Total Length: ' + str(len(data[key])))
            if len(data[key]) > 0:
                mogrifyRes = self.db.execute_mogrify(data[key],key,insertTables=self.tableToInsert)
            else:
                mogrifyRes = 1
            if mogrifyRes == 0:
                self.data[key].clear()
        return