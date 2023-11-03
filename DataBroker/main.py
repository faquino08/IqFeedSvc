import logging
import inspect
import datetime
import time
import pytz
import psycopg2
import psycopg2.extras
import pandas as pd
import pandas.io.sql as sqlio
from DataBroker.Sources.IQFeed.iqfunctions import iqFunctions as iq
from sqlalchemy import create_engine
from constants import DEFAULT_MIN_START, DEFAULT_DAILY_START, APP_NAME
from DataBroker.Sources.IQFeed.database import databaseHandler

params = {
    "host": '',
    "port": '',
    "database": '',
    "user": '',
    "password": ''
}

class Main:
    def __init__(self,postgresParams={},debug=False,tablesToInsert=[],symbolTables={},assetTypes={},iqHost="127.0.0.1",iqPort=9101):
        '''
        Main class for running different workflows for IQ Feed API requests.
        postgresParams -> (dict) Dict with keys host, port, database, user, \
                            password for Postgres database
        debug -> (boolean) Whether to record debug logs
        tablesToInsert -> (list) List of string with table names to insert
        symbolTables -> (dict) Dict where values are name of tables to \
                            update and one key must be 'Uni' for universe of \
                            symbols
        assetTypes -> (dict) Dict where values are asset types for each \
                            key in symbolTables and keys must be the same for \
                            both
        iqHost -> (str) Host address of IQFeed
        iqPort -> (int) Port of IQFeed Socket
        '''
        nyt = pytz.timezone('America/New_York')
        self.today = datetime.datetime.now(tz=nyt).date()
        self.data = {}
        if debug:
            logging.basicConfig(
                level=logging.DEBUG,
                format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
                datefmt="%m-%d %H:%M",
                #handlers=[logging.FileHandler(f'./logs/output_{datetime.date.today()}.txt'), logging.StreamHandler()],
            )
        else:
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
                datefmt="%m-%d %H:%M",
                #handlers=[logging.FileHandler(f'./logs/output_{datetime.date.today()}.txt')],
            )
        self.log = logging.getLogger(__name__)
        self.params = postgresParams
        self.startTime = time.time() 
        self.log.info(f'')
        self.log.info(f'')
        self.log.info(f'')
        self.log.info(f'Main')
        self.log.info(f'Starting Run at: {self.startTime}')
        self.connect()
        caller = 'IQ' + inspect.stack()[1][3].upper()
        self.db.cur.execute('''
            INSERT INTO PUBLIC.financedb_RUNHISTORY ("Process","Startime") VALUES ('%s','%s') RETURNING "Id";
        ''' % (caller,self.startTime))
        self.runId = self.db.cur.fetchone()[0]
        self.db.conn.commit()
        try:
            self.iq = iq(self.db,debug,True,iqHost,iqPort,tablesToInsert=tablesToInsert)
        except ValueError as error:
            self.log.error(error)
            raise

        lastDate = self.iq.db.getLastDate('tdequityfrequencytable','added')
        if lastDate != self.today or lastDate == None:
            if symbolTables.keys() == assetTypes.keys():
                self.queueSecurities(symbolTables,assetTypes)
                self.makeTdFrequencyTable()
                #self.makePowerAutomateInput()
            else:
                return self.log.error("Non Matching Keys in symbolTables & assetTypes")

    def exit(self):
        '''
        Exit class. Log Runtime. And shutdown logging.
        '''
        self.iq.exit()
        self.endTime = time.time()
        # Update RunHistory With EndTime
        self.db.cur.execute('''
            UPDATE PUBLIC.financedb_RUNHISTORY
            SET "Endtime"=%s,
                "SymbolsInsert"=%s,
                "LastSymbol"='%s'
            WHERE "Id"='%s'
        ''' % (self.endTime,self.totalLen,self.lastSymbol,self.runId))
        self.db.conn.commit()
        self.db.exit()
        #self.cur.close()
        #self.conn.close()
        #self.log.info('Db Exit Status:')
        #self.log.info('Psycopg2:')
        #self.log.info(self.conn.closed)
        self.log.info("Closing Main")
        self.log.info(f'Ending Run at: {self.endTime}')
        self.log.info(f'Runtime: {self.endTime - self.startTime}')
        logging.shutdown()

    def runIqRequests(self,minute=False,daily=False,minbeginDate="",daybeginDate="",fullMarket=False):
        '''
        Function for running IQ Feed API requests 
        minute -> (boolean) Whether to run minute price history flow
        daily -> (boolean) Whether to run daily price history flow
        minbeginDate -> (str) Datetime to begin minute price lookup \
                    'YYYY-MM-dd HH:MM:SS'
        daybeginDate -> (str) Datetime to begin daily price lookup \
            'YYYY-MM-dd HH:MM:SS'
        '''
        self.log.info('Minute: ' + str(minute))
        priceHist = False
        if minute or daily:
            priceHist = True
        if not fullMarket:
            self.getTotalFreqTable("Iq",priceHist=priceHist)
            iqSymbols = self.equityFreqTable[['freq']]
            iqOptionable = self.optionableFreqTable[['freq']]
        else:
            self.getTotalFreqTable("Iq")
            iqSymbols = self.equityFreqTable[['freq']]
            iqOptionable = self.optionableFreqTable[['freq']]
        iqSymbols['asset_type'] = 'EQUITY'
        iqOptionable['asset_type'] = 'EQUITY'
        #iqOptionable = self.makeOptionableTotalFreqTable("Iq")
        #iqSymbols = self.makeTotalFreqTable("Iq")
        # Update RunHistory With Length Of Queue
        self.totalLen = 0
        if minute and daily:
            self.totalLen = len(iqSymbols) + len(iqOptionable)
        elif minute:
            self.totalLen = len(iqOptionable)
        elif daily:
            self.totalLen = len(iqSymbols)
        self.db.cur.execute('''
            UPDATE PUBLIC.financedb_RUNHISTORY
            SET "SymbolsToFetch"=%s,
                "Notes"='Important: %s Optionable: %s'
            WHERE "Id"=%s
        ''' % (str(self.totalLen),len(iqSymbols),len(iqOptionable),self.runId))
        self.db.conn.commit()
        if minute:
            self.iq.db.cur.execute('''
                SELECT "symbol",MAX("date") FROM "iqpricehistory_min" GROUP BY "symbol"
            ''')
            colNames = ['Symbol','Date']
            lastDateTable = pd.DataFrame(self.iq.db.cur.fetchall(),columns=colNames).set_index("Symbol")
            if len(lastDateTable) > 0:
                lastDateTable['Date'] = lastDateTable['Date'].dt.strftime('%Y%m%d %H%M')
                self.log.info('LastDateTable: ' + str(lastDateTable))
            if len(minbeginDate) <= 0:
                startDate = DEFAULT_MIN_START
            else:
                startDate = minbeginDate[0:minbeginDate.find(".")].replace("T"," ")
                startDate = startDate.replace("-","")
                startDate = startDate.replace(":","")
            syms = iqOptionable.index.values
            self.iq.makeRequests(syms,'price','iqpricehistory_min',startDate,lastDateTable)
        if daily:
            self.iq.db.cur.execute('''
                SELECT "symbol",MAX("date") FROM "iqpricehistory_daily" GROUP BY "symbol"
            ''')
            colNames = ['Symbol','Date']
            lastDateTable = pd.DataFrame(self.iq.db.cur.fetchall(),columns=colNames).set_index("Symbol")
            lastDateTable['Date'] = lastDateTable['Date'].astype('datetime64[us]').dt.strftime('%Y%m%d')
            self.log.info('LastDateTable: ' + str(lastDateTable))
            if len(daybeginDate) <= 0:
                startDate = DEFAULT_DAILY_START
            else:
                startDate = daybeginDate.replace("-","")
            syms = iqSymbols.index.values
            self.iq.makeRequests(syms,'price','iqpricehistory_daily',startDate,lastDateTable)
        self.lastSymbol = syms[-1]
        return

    def queueSecurities(self,sqlTables={"Uni": "listedsymbols"},assetTables={"Uni":"EQUITY"}):
        '''
        Get securities and indices components from Postgres.
        sqlTables -> (dict) Dict where values are tables of symbols \
                            universe and indices. MUST HAVE 'Uni' KEY
        assetTables -> (dict) Dict where values are asset types for each \
                            key in sqlTables and keys must be the same for \
                            both
        '''
        self.symbols = {}
        self.log.info("Weekday: " + str(self.today.isoweekday()))
        if self.today.isoweekday() == 1:
            filterDay = self.today - datetime.timedelta(3)
            moverFilterDay = filterDay
        elif self.today.isoweekday() > 5:
            filterDay = self.today - datetime.timedelta(self.today.isoweekday()-1)
            moverFilterDay = filterDay
        else: 
            filterDay = self.today - datetime.timedelta(1)
            moverFilterDay = filterDay - datetime.timedelta(1)
        filterDay = filterDay.strftime("%m/%d/%Y")
        moverFilterDay = moverFilterDay.strftime("%m/%d/%Y")
        for key in sqlTables.keys():
            getSql = "SELECT * FROM \"%s\" ORDER BY \"Symbol\"" % sqlTables[key]
            if key != "Uni":
                getSql = "SELECT * FROM \"%s\" WHERE \"Updated\" = '%s' ORDER BY \"Symbol\"" % (sqlTables[key], filterDay)
            if key == "Movers":
                getSql = "SELECT * FROM \"%s\" WHERE DATE(\"dateadded\" AT TIME ZONE 'US/Eastern') = '%s' ORDER BY \"Symbol\"" % (sqlTables[key], moverFilterDay)
            self.symbols[key] = sqlio.read_sql_query(getSql,self.connAlch,index_col="Symbol")
            self.symbols[key]["asset_type"] = assetTables[key]
            self.log.info(key)
            self.symbols[key].loc[self.symbols[key].index.str[0] == '/',"asset_type"] = "FUTURE"
        return

    def makeTdFrequencyTable(self,broker="Td",excludeUni=True):
        '''
        Create tabulation of frequencies in indices.
        excludeUni -> (boolean) Whether to count the symbols universe in the \
                        the tabulation
        '''
        missingSymbols ={}
        symUni = self.symbols['Uni'].loc[self.symbols['Uni']['asset_type'].str[0:] == 'EQUITY',["SymbolTd","SymbolIb","asset_type","Security Name"]]
        tdMainCount = symUni["SymbolTd"].value_counts()
        if excludeUni:
            tdMainCount = tdMainCount.replace(1,0)
        freqTable = pd.DataFrame({"SymbolTd": symUni["SymbolTd"].array, "Symbol": symUni.index.array, "SymbolIb": symUni["SymbolIb"].array, "asset_type": symUni["asset_type"].array, "Security Name": symUni["Security Name"].array})
        freqTable['Sector'] = ''
        freqTable['Industry'] = ''
        freqTable['Sub-Industry'] = ''
        freqTable = freqTable.set_index('SymbolTd')
        for key in self.symbols.keys():
            if (key != "Uni" and 'tdscan' in key):
                sym = self.symbols[key].loc[self.symbols[key]['asset_type'].str[0:] == 'EQUITY']
                count = sym.index.value_counts()
                tdMainCount = tdMainCount.add(count,axis=0,fill_value=0)
                countIndices = count.index.str[:]
                if len(freqTable) > 0:
                    mainTableIndices = freqTable.index.str[:].values
                else:
                    mainTableIndices = []
                if 'tdscan' in key:
                    colName = key[:-7]
                else:
                    colName = key
                freqTable[colName] = 0
                for index, item in tdMainCount.iteritems():
                    if index not in mainTableIndices:
                        newRow = pd.DataFrame([[self.tdtoIqSymbol(index), self.tdtoIbSymbol(index), str(self.symbols[key].loc[index,"Description"])]],index=[index],columns=["Symbol","SymbolIb","Security Name"])
                        freqTable = pd.concat([freqTable,newRow])
                        missingSymbols[index] = key
                for index, row in freqTable.iterrows():
                    if index in countIndices:
                        freqTable.loc[index,colName] = 1
            elif key == "Movers":
                sym = self.symbols[key].loc[self.symbols[key]['asset_type'].str[0:] == 'EQUITY']
                count = sym.index.value_counts()
                tdMainCount = tdMainCount.add(count,axis=0,fill_value=0)
                countIndices = count.index.str[:]
                mainTableIndices = freqTable.index.str[:].values
                if 'tdscan' in key:
                    colName = key[:-7]
                else:
                    colName = key
                freqTable[colName] = 0
                for index, item in tdMainCount.iteritems():
                    if index not in mainTableIndices:
                        newRow = pd.DataFrame([[self.tdtoIqSymbol(index), self.tdtoIbSymbol(index), str(self.symbols[key].loc[index,"Description"])]],index=[index],columns=["Symbol","SymbolIb","Security Name"])
                        freqTable = pd.concat([freqTable,newRow])
                        missingSymbols[index] = key
                for index, row in freqTable.iterrows():
                    if index in countIndices:
                        freqTable.loc[index,colName] = 1
            elif key == "Sectors":
                sym = self.symbols[key].loc[self.symbols[key]['asset_type'].str[0:] == 'EQUITY']
                count = sym.index.value_counts()
                tdMainCount = tdMainCount.add(count,axis=0,fill_value=0)    
                countIndices = count.index.str[:]
                mainTableIndices = freqTable.index.str[:].values
                if 'tdscan' in key:
                    colName = key[:-7]
                else:
                    colName = key
                freqTable[colName] = 0
                for index, item in tdMainCount.iteritems():
                    if index not in mainTableIndices:
                        newRow = pd.DataFrame([[self.tdtoIqSymbol(index), self.tdtoIbSymbol(index), str(self.symbols[key].loc[index,"Description"])]],index=[index],columns=["Symbol","SymbolIb","Security Name"])
                        freqTable = pd.concat([freqTable,newRow])
                        missingSymbols[index] = key
                for index, row in freqTable.iterrows():
                    if index in sym.index:
                        freqTable.loc[index,"Sector"] = self.symbols[key].loc[index,"Sector"]
                        freqTable.loc[index,"Industry"] = self.symbols[key].loc[index,"Industry"]
                        freqTable.loc[index,"Sub-Industry"] = self.symbols[key].loc[index,"Sub-Industry"]
                        freqTable.loc[index,colName] = 1
                     
        freqTable = freqTable.fillna(0)
        freqTable['asset_type'] = 'EQUITY'
        freqTable.reset_index(inplace=True)
        freqTable.rename(columns={'index':'SymbolTd'},inplace=True)
        self.iq.db.createTable(freqTable,"tdEquityFrequencyTable",['"added" date default now()'] ,drop=True)
        self.iq.db.execute_mogrify(freqTable.values.tolist(),"tdEquityFrequencyTable",insertTables=['tdEquityFrequencyTable'])
        return

    def makeTotalFreqTable(self,broker="Td"):
        '''
        Create table of symbols sorted by frequency in indices.
        broker -> (str) Either 'Td' (TD Ameritrade), 'Ib' \
                    (Interactive Brokers), or 'Iq' (IQ Feed) for which symbol \
                    style to use
        '''
        if broker=="Td":
            index = "SymbolTd"
        elif broker=="Ib":
            index = "SymbolIb"
        elif broker=="Iq":
            index = "Symbol"
        self.equityFreqTable = sqlio.read_sql_query('SELECT * FROM TDEQUITYFREQUENCYTABLE', self.connAlch,index_col=index)
        count = self.equityFreqTable.sum(axis=1,numeric_only=True)
        count = count[count != 0]
        count = count.to_frame("freq")
        count = count["freq"].sort_values(ascending=False)
        res = pd.DataFrame()
        res["freq"] = 0
        res["asset_type"] = 'EQUITY'
        
        for index, value in count.iteritems():
            res.loc[index] = {'freq': value, 'asset_type': 'EQUITY'}
        return res

    def getTotalFreqTable(self,broker="Td",priceHist=False,fundamentals=False):
        '''
        Create table of symbols sorted by frequency in indices.
        broker -> (str) Either 'Td' (TD Ameritrade), 'Ib' \
                    (Interactive Brokers), or 'Iq' (IQ Feed) for which symbol \
                    style to use
        '''
        if broker=="Td":
            index = "SymbolTd"
        elif broker=="Ib":
            index = "SymbolIb"
        elif broker=="Iq":
            index = "Symbol"
        self.iq.db.cur.execute(
            '''
                SELECT Max("dateadded")
                FROM PUBLIC."tdfundamentaldata"
            ''')

        startdate = self.iq.db.cur.fetchone()[0]
        if startdate is None:
            startdate = self.today - datetime.timedelta(180)
            startdate = startdate.strftime('%Y-%m-%d %H:%M')

        try:
            colNames = [index,"freq"]
            # Get Total Frequency table (priceHist focuses on optionable and those in indices)
            if fundamentals:
                colNames = [index,"freq","Time","Event"]
                if self.today.isoweekday() == 5:
                    # Returns symbols whose earnings has recently occured or that will occur in the next three days
                    self.iq.db.cur.execute(
                    '''
                        WITH X AS
                        (
                            SELECT 
                                "%s","Dji"+"Nasd100"+"Optionable"+"PennyOptions"+"Russell"+"Sp400"+"Sp500"+"WeeklyOptions"+"Movers"+"Sectors" as "freq"
                            FROM PUBLIC."tdequityfrequencytable"
                        ),Y AS (
                            SELECT 
                                row_number() OVER (PARTITION BY "Symbol" ORDER BY "Time"),"Time","Symbol","Event"
                            FROM PUBLIC."calendar_tdscan"
                            WHERE "Event"='Earnings'
                        )
                        SELECT 
                            t1."%s",t1."freq",t2."Time",t2."Event"
                        FROM X t1
                            LEFT JOIN Y t2
                                ON t1."%s"=t2."Symbol"
                        WHERE 
                            ("Time"='%s' 
                                AND 
                                ("Time" AT TIME ZONE 'America/New_York')<=CURRENT_TIMESTAMP)
                            OR
                            (("Time" AT TIME ZONE 'America/New_York')       >=CURRENT_TIMESTAMP
                                AND
                                ("Time" AT TIME ZONE 'America/New_York')<=CURRENT_TIMESTAMP + INTERVAL '3' DAY)
                            OR
                            "Time" is null
                        ORDER BY "Time","freq" DESC
                    ''' % (index,index,index,startdate))
                else:
                    # Returns symbols whose earnings has recently occured or that will occur in the next day
                    self.iq.db.cur.execute(
                        '''
                            WITH X AS
                            (
                                SELECT 
                                    "%s","Dji"+"Nasd100"+"Optionable"+"PennyOptions"+"Russell"+"Sp400"+"Sp500"+"WeeklyOptions"+"Movers"+"Sectors" as "freq"
                                FROM PUBLIC."tdequityfrequencytable"
                            ),Y AS (
                                SELECT 
                                    row_number() OVER (PARTITION BY "Symbol" ORDER BY "Time"),"Time","Symbol","Event"
                                FROM PUBLIC."calendar_tdscan"
                                WHERE "Event"='Earnings'
                            )
                            SELECT 
                                t1."%s",t1."freq",t2."Time",t2."Event"
                            FROM X t1
                                LEFT JOIN Y t2
                                    ON t1."%s"=t2."Symbol"
                            WHERE 
                                ("Time"='%s' 
                                    AND 
                                    ("Time" AT TIME ZONE 'America/New_York')<=CURRENT_TIMESTAMP)
                                OR
                                (("Time" AT TIME ZONE 'America/New_York')       >=CURRENT_TIMESTAMP
                                    AND
                                    ("Time" AT TIME ZONE 'America/New_York')<=CURRENT_TIMESTAMP + INTERVAL '1' DAY)
                                OR
                                "Time" is null
                            ORDER BY "Time","freq" DESC
                        ''' % (index,index,index,startdate))
            elif priceHist:
                # Returns Symbols that are optionable, movers, or are in major indices
                self.iq.db.cur.execute(
                    '''
                        WITH X AS (
                            SELECT 
                                "%s","Dji","Nasd100","Optionable","PennyOptions","Russell","Sp400","Sp500","WeeklyOptions","Movers",
		                        "Dji"+"Nasd100"+"Optionable"+"PennyOptions"+"Russell"+"Sp400"+"Sp500"+"WeeklyOptions"+"Movers"+"Sectors" as "freq"
                            FROM PUBLIC."tdequityfrequencytable"
                            ORDER BY "freq" DESC
                        )
                        SELECT "%s","freq"
                        FROM X
                        WHERE "freq">0 
                            and ("Optionable"=1 or "Dji"=1
                                or "Nasd100"=1 or "Russell"=1
                                or "Sp400"=1 or "Sp500"=1  or "Movers"=1
                                or "PennyOptions"=1 or "WeeklyOptions"=1)
                    ''' % (index,index))
            else:
                # Returns every symbol in universe
                self.iq.db.cur.execute(
                    '''
                        SELECT 
                            "%s","Dji"+"Nasd100"+"Optionable"+"PennyOptions"+"Russell"+"Sp400"+"Sp500"+"WeeklyOptions"+"Movers"+"Sectors" as "freq"
                        FROM PUBLIC."tdequityfrequencytable"
                        ORDER BY "freq" DESC
                    ''' % (index))
            self.equityFreqTable = pd.DataFrame(self.iq.db.cur.fetchall(),columns=colNames).set_index(index)
            self.iq.db.conn.commit()
            
            # Get Optionable frequency table
            if not fundamentals:
                self.iq.db.cur.execute(
                    '''
                    SELECT 
                        "%s","Dji"+"Nasd100"+"Optionable"+"PennyOptions"+"Russell"+"Sp400"+"Sp500"+"WeeklyOptions"+"Movers"+"Sectors" as "freq"
                    FROM PUBLIC."tdequityfrequencytable"
                    WHERE "Optionable" = 1
                    ORDER BY "freq" DESC
                    ''' % (index))
                self.optionableFreqTable = pd.DataFrame(self.iq.db.cur.fetchall(),columns=colNames).set_index(index)
                self.iq.db.conn.commit()
            self.log.info(self.equityFreqTable)
        except (Exception, psycopg2.DatabaseError) as error:
            self.log.error("Error: %s" % error)
            self.iq.db.conn.rollback()
        '''self.equityFreqTable = sqlio.read_sql_query('SELECT * FROM TDEQUITYFREQUENCYTABLE', self.connAlch,index_col=index)
        for colName in colNames:
            if (colName in self.db.bigint_list) or (colName in self.db.decimal_list):
                self.equityFreqTable[colName] = self.equityFreqTable[colName].astype(float)
        count = self.equityFreqTable.sum(axis=1,numeric_only=True)
        count = count[count != 0]
        count = count.to_frame("freq")
        count = count["freq"].sort_values(ascending=False)
        res = pd.DataFrame()
        res["freq"] = 0
        res["td_service_name"] = 'QUOTE'
        
        for index, value in count.iteritems():
            res.loc[index] = {'freq': value, 'td_service_name': 'QUOTE'}
        return res
        '''
        return

    def makeOptionableTotalFreqTable(self,broker="Td"):
        '''
        Create table of symbols sorted by frequency in indices. All returned symbols must optionable.
        broker -> (str) Either 'Td' (TD Ameritrade), 'Ib' \
                    (Interactive Brokers), or 'Iq' (IQ Feed) for which symbol \
                    style to use
        '''
        if broker=="Td":
            index = "SymbolTd"
        elif broker=="Ib":
            index = "SymbolIb"
        elif broker=="Iq":
            index = "Symbol"
        self.equityFreqTable = sqlio.read_sql_query('SELECT * FROM TDEQUITYFREQUENCYTABLE', self.connAlch,index_col=index)
        optionable = self.equityFreqTable[self.equityFreqTable.Optionable == 1]
        count = optionable.sum(axis=1,numeric_only=True)
        count = count[count > 1]
        count = count.to_frame("freq")
        count = count["freq"].sort_values(ascending=False)
        res = pd.DataFrame()
        res["freq"] = 0
        res["asset_type"] = 'EQUITY'
        
        for index, value in count.iteritems():
            res.loc[index] = {'freq': value, 'asset_type': 'EQUITY'}
        return res

    def tdtoIbSymbol(self,tdSymbol=''):
        '''
        Function for converting TD Ameritrade symbol to Interactive Brokers
        tdSymbol -> Symbol to convert
        '''
        ibSymbol = tdSymbol.replace("/WS"," WAR",)
        ibSymbol = ibSymbol.replace("/U"," U")
        ibSymbol = ibSymbol.replace("p"," PR")
        return ibSymbol

    def tdtoIqSymbol(self,tdSymbol=''):
        '''
        Function for converting TD Ameritrade symbol to IQ Feed
        tdSymbol -> Symbol to convert
        '''
        iqSymbol = tdSymbol.replace("/",".")
        return iqSymbol

    def connect(self):
        '''
        Connect to the PostgreSQL database server
        '''       
        try:
            # connect to the PostgreSQL server
            #self.log.debug('Connecting to the PostgreSQL database...')
            self.log.info(self.params)
            self.log.info(params)
            if not all(x in self.params for x in params):
                raise ValueError(f'Main did not receive a valid value for postgresParams. Need to receive dict in format {params}')
            self.db = databaseHandler(self.params)
            #self.conn = psycopg2.connect(**self.params)
            alchemyStr = f"postgresql+psycopg2://{self.params['user']}:{self.params['password']}@{self.params['host']}:{self.params['port']}/{self.params['database']}?application_name={APP_NAME}_database_Alchemy"
            self.connAlch = create_engine(alchemyStr).connect()
            #self.cur = self.conn.cursor()
            self.log.debug("Connection successful")
            return
        except (Exception, psycopg2.DatabaseError) as error:
            self.log.error(error)
            return