import sys
import psycopg2
import psycopg2.extras
import os
from os.path import exists
import csv
import datetime
import time
import logging
import locale
from io import StringIO

logger = logging.getLogger(__name__)
class databaseHandler:
    def __init__(self,params_dic={}):
        '''
        Database wrapper for common SQL queries and handling database connection.
        params_dic -> Dict with keys host, port, database, user, \
                            password for Postgres database
        logger -> logging object
        '''
        locale.setlocale( locale.LC_ALL, 'en_US.UTF-8' )
        self.params = params_dic
        self.logger = logger
        self.conn = None
        self.cur = None
        self.batch_size = 1000000
        self.connect()
        self.tableNames = []
        self.char_list = [
        ]
        self.chartwo_list =[
        ]
        self.symbol_list = [
        ]
        self.uniqueSymbol_list = [
            "Symbol",
            "SymbolTd"
        ]
        self.bigint_list = [
            "Volume",
            "Market Cap",
        ]
        self.decimal_list = [
            "Last",
            "Vol Index",
            "Net Chng",
            "%Change",
            "Bid",
            "Ask",
            "High",
            "Low",
            "EPS",
            "Dji",
            "Nasd100",
            "Optionable",
            "PennyOptions",
            "Russell",
            "Sp400",
            "Sp500",
            "WeeklyOptions",
            "Movers",
            "Sectors"
        ]
        self.name_list = [
            "Description",
            "Security Name"
        ]
        self.date_list = [
            "Expiration Date"
        ]
    
    def exit(self):
        '''
        Exit class and close Postgres connection
        '''
        self.cur.close()
        self.conn.close()
        self.logger.info('Db Exit Status:')
        self.logger.info('Psycopg2:')
        self.logger.info(self.conn.closed)

    def copy_from_stringio(self, csv, table):
        '''
        Read txt file and insert into Postgres Table.
        file -> csv file to read
        table -> Table Name in Postgres
        '''
        # save dataframe to an in memory buffer
        buffer = StringIO(csv)
        buffer.seek(0)
        try:
            self.cur.copy_from(buffer, table, sep=",")
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error("Error: %s" % error)
            self.conn.rollback()
            return 1
        self.logger.info("copy_from_stringio() done")

    def connect(self):
        '''
        Connect to the PostgreSQL database server 
        '''
        try:
            # connect to the PostgreSQL server
            self.logger.debug('Connecting to the PostgreSQL database...')
            self.conn = psycopg2.connect(**self.params)
            self.cur = self.conn.cursor()
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.debug(error)
            sys.exit(1)
        self.logger.debug("Connection successful")
        return

    def composeSqlColumnsPlaceholders(self,dataSample=[]):
        '''
        Takes list and create string of placeholders for each entry in list for execute_mogrify.
        dataSample -> (list) List of columns to insert
        '''
        result = "("
        i = 1
        while i <= len(dataSample):
            result += "%s,"
            i += 1
        result = result[:-1] + ")"
        return result
   
    def execute_mogrify(self,index,table=None,insertTables=[]):
        '''
        Takes dataframe and inserts the data into the provided table using execute_mogrify.
        index -> (Dataframe) data to insert into database
        table -> (str) Name of Postgres Table
        insertTables -> (list) List of string for each table name to insert
        '''
        if table is not None:
            deleteLength = 0
            toDelete = []
            if table == "iqpricehistory_daily":
                toInsert = [x  for x in index if len(x) == 8]
                length = len(toInsert)
                if (length > 0):
                    str_placholders = self.\
                        composeSqlColumnsPlaceholders(dataSample=toInsert[0])
                    for i in range(0, length, self.batch_size):
                        if ((i+self.batch_size) < length):
                            batch_max = i+self.batch_size
                        elif (i+self.batch_size >= length):
                            batch_max = length
                        if length == 1:
                            batch_max = 1
                        values = [self.cur.mogrify(str_placholders, tup).decode('utf8') for tup in toInsert[i:batch_max]]
                        args_str = ",".join(values)
                        args_str = args_str.replace("\'NULL\'","NULL")
                        self.logger.info("Inserting daily price data from IQFeed...")
                        self.logger.debug(f'INSERT INTO public.{table} VALUES{args_str};')
                        try:
                            self.cur.execute(f'INSERT INTO public.{table} VALUES{args_str};')
                            self.conn.commit()
                            for j in index:
                                if len(j) != 8:
                                    self.logger.info(j)
                        except (Exception, psycopg2.DatabaseError) as error:
                            self.logger.error("Error: %s" % error)
                            self.conn.rollback()
            elif table == "iqpricehistory_min":
                toInsert = [x  for x in index if len(x) == 8]
                length = len(toInsert)
                if (length > 0):
                    str_placholders = self.\
                        composeSqlColumnsPlaceholders(dataSample=index[0])
                    for i in range(0, len(index), self.batch_size):
                        if ((i+self.batch_size) < len(index)):
                            batch_max = i+self.batch_size
                        elif (i+self.batch_size >= len(index)):
                            batch_max = len(index)
                        if len(index) == 1:
                            batch_max = 1
                        try:    
                            values = [self.cur.mogrify(str_placholders, tup).decode('utf8') for tup in index[i:batch_max]]
                        except:
                            self.logger.error("Values error ")
                            self.logger.error(str(tup) for tup in index[i:batch_max])
                            return
                        args_str = ",".join(values)
                        args_str = args_str.replace("\'NULL\'","NULL")
                        self.logger.info("Inserting minute price data from IQFeed...")
                        self.logger.debug(f'INSERT INTO public.{table} VALUES{args_str};')
                        try:
                            self.cur.execute(f'INSERT INTO public.{table} VALUES{args_str};')
                            self.conn.commit()
                            for j in index:
                                if len(j) != 8:
                                    self.logger.info(j)
                        except (Exception, psycopg2.DatabaseError) as error:
                            self.logger.error("Error: %s" % error)
                            self.conn.rollback()
            elif table in insertTables:
                if (len(index) > 0):
                    str_placholders = self.\
                        composeSqlColumnsPlaceholders(dataSample=index[0])
                    for i in range(0, len(index), self.batch_size):
                        batch_max = i+self.batch_size
                        if ((i+self.batch_size) < len(index)):
                            batch_max = i+self.batch_size
                        elif (i+self.batch_size >= len(index)):
                            batch_max = len(index)
                        if len(index) == 1:
                            batch_max = 1
                        values = [self.cur.mogrify(str_placholders, tup).decode('utf8') for tup in index[i:batch_max]]
                        #self.logger.info("Values: " + str(values))
                        args_str = ",".join(values)
                        args_str = args_str.replace("\' \'","NULL")
                        args_str = args_str.replace("\'Null\'","NULL")
                        args_str = args_str.replace("\'NULL\'","NULL")
                        args_str = args_str.replace("None",'"NULL"')
                        args_str = args_str.replace("up",'UP')
                        args_str = args_str.replace("down",'DOWN')
                        self.logger.info('Inserting into %s' % table)
                        self.logger.info(f'INSERT INTO public.{table} VALUES{args_str};')
                        try:
                            self.cur.execute(f'INSERT INTO public.{table} VALUES{args_str};')
                            self.conn.commit()
                            return 0
                        except (Exception, psycopg2.DatabaseError) as error:
                            self.logger.error("Error: %s" % error)
                            self.conn.rollback()
        else:
            raise Exception("Need to provide table for execute_mogrify.")

    def getLastDate(self,table,column,iqFormat=False):
        '''
        Get largest date in Postgres table.
        table -> (str) Name of Postgres table
        column -> (str) Name of Postgres date column
        iqFormat -> (boolean) Whether to format date for IQFeed
        '''
        lastTimeSql = "SELECT MAX(\"{}\") FROM {}"
        sqlComm = lastTimeSql.format(column,table)
        try:
            self.cur.execute(sqlComm)
            lastDate = self.cur.fetchone()[0]
            if iqFormat == True and lastDate != None:
                lastDate = str(lastDate).replace('-','')
                lastDate = lastDate.replace(':','')
            return lastDate
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error("Error: %s" % error)
            self.conn.rollback()
            return None

    def getLastDateForSymbol(self,table,column,symCol,symbol):
        '''
        Get largest date in Postgres table for specific entry.
        table -> (str) Name of Postgres table
        column -> (str) Name of Postgres date column
        symCol -> (str) Name of column to filter
        symbol -> (str) Value to filter for in table
        '''
        lastTimeSql = "SELECT MAX(\"{}\") FROM {} WHERE \"{}\"='{}'"
        sqlComm = lastTimeSql.format(column,table,symCol,symbol)
        try:
            self.cur.execute(sqlComm)
            lastDate = self.cur.fetchone()[0]
            return lastDate
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error("Error: %s" % error)
            self.conn.rollback()
            return None

    def createTable(self,panda,table=None,addlCols=[],drop=False):
        '''
        Create new empty table based on columns in Dataframes.
        panda -> (Dataframe) Data to insert in Postgres
        table -> (str) Name of Postgres table
        addlCols -> (list) List of additional columns to insert not in panda
        drop -> (boolean) Whether to drop old table or not
        '''
        # Drop Old Table
        if drop:
            drop_sql = f"DROP TABLE IF EXISTS {table};"
            self.logger.info(drop_sql)
            try:
                self.cur.execute(drop_sql)
                self.conn.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                self.log.error("Error: %s" % error)
                self.conn.rollback()
        # Create New Table
        if table is not None:
            #cols = self.createTableColSql(self.csv_cols,addlCols=addlCols)
            self.csv_cols = self.getColNamesDataTypes(panda,addlCols=addlCols)
            sqlCom = "CREATE TABLE IF NOT EXISTS %s %s" % (table,self.csv_cols)
            self.logger.info(sqlCom)
            try:
                self.cur.execute(sqlCom)
                self.conn.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                self.logger.error("Error: %s" % error)
                self.conn.rollback()
            return

    def getColNamesDataTypes(self,panda,unique=True,excluded=[],addlCols=[]):
        '''
        Takes pandas dataframe and returns string formatting the columns for SQL Create Table query.
        excluded -> (list) Columns from Dataframe to exclude from Sql query
        addlCols -> (list) Additional columns to insert on top of Dataframe\ 
                        columns
        '''
        res = "("
        numOfPk = 0
        # Loop Through Columns and Append to
        for col in panda.columns:
            notExcluded = bool(col not in excluded)
            if (notExcluded):
                if col in self.symbol_list:
                    res += "%s varchar(14)," % '"{}"'.format(col)
                elif col in self.uniqueSymbol_list and numOfPk < 1:
                    res += "%s varchar(14) primary key," % '"{}"'.format(col)
                    numOfPk += 1
                elif col in self.name_list :
                    res += "%s varchar(255)," % '"{}"'.format(col)
                elif col in self.char_list:
                    res += "%s varchar(1)," % '"{}"'.format(col)
                elif col in self.chartwo_list:
                    res += "%s varchar(2)," % '"{}"'.format(col)
                elif col in self.bigint_list:
                    res += "%s bigint," % '"{}"'.format(col)
                elif col in self.date_list:
                    res += "%s date," % '"{}"'.format(col)
                elif col in self.decimal_list:
                    res += "%s decimal," % '"{}"'.format(col)
                else:
                    res += "%s text," % '"{}"'.format(col)
        if len(addlCols) > 0:
            for col in addlCols:
                notExcluded = bool(col not in excluded)
                if (notExcluded):
                    res += "%s," % col
        res = str(res[:-1]) + ")"
        return res

    def getNewIndexEntries(self):
        '''
        Go through master index file and search for entries not in our database.
        '''
        file_exists = exists("./index/master.csv")
        if (file_exists):
            with open("./index/master.csv") as file:
                # Passing the TSV file to 
                # reader() function
                # with tab delimiter
                # This function will
                # read data from file
                tsv_file = csv.reader(file, delimiter="|")

                # Getting today's date
                todays_Date = datetime.date.fromtimestamp(time.time())

                lastEntry = self.getLastDate("edgarindex","FILING_DATE")
                if lastEntry is not None:
                    self.tsv_results = [row for row in tsv_file if datetime.date.fromisoformat(row[3]) > lastEntry and datetime.date.fromisoformat(row[3])+datetime.timedelta(days=2)  <= todays_Date]
                else:
                    self.tsv_results = list(tsv_file)

                #print("csv to list:",index)
            self.execute_mogrify(self.tsv_results,"edgarindex")
            os.system('cd ./index & del master.csv &')
            #Linux
            #os.system('cd ./index & rm -rf master.csv &')
        else:
            self.logger.info("")
            self.logger.info("No Master.csv to insert")
        return