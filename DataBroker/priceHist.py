from DataBroker.main import Main
from DataBroker.Sources.SymbolsUniverse.holidayCalendar import getHolidaySchedule
import pytz
from datetime import datetime
from constants import POSTGRES_LOCATION, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, IQ_LOCATION, IQ_PORT, DEBUG, APP_NAME

def priceHist(minbeginDate="",daybeginDate="",fullMarket=False,auto=False):
    '''
    Wrapper function to request price history data from IQ Feed.
    debug -> (boolean) Whether to log debug messages
    minbeginDate -> (str) Datetime to begin minute price lookup \
                    'YYYY-MM-dd HH:MM:SS'
    daybeginDate -> (str) Datetime to begin daily price lookup \ 
                    'YYYY-MM-dd HH:MM:SS'
    '''
    debug=DEBUG
    holidays = getHolidaySchedule()
    utc = pytz.timezone('UTC')
    est = pytz.timezone('US/Eastern')
    time = datetime.utcnow().replace(tzinfo=utc)
    localTime = time.astimezone(est)
    localDate = localTime.date()
    main = Main(
        postgresParams={
            "host": f'{POSTGRES_LOCATION}',
            "port": f'{POSTGRES_PORT}',
            "database": f'{POSTGRES_DB}',
            "user": f'{POSTGRES_USER}',
            "password": f'{POSTGRES_PASSWORD}',
            "application_name": f'{APP_NAME}PriceHist'
        },
        debug=debug,
        tablesToInsert=["iqpricehistory_min","iqpricehistory_daily"],
        symbolTables={
            "Uni": "listedsymbols",
            "Dji_tdscan":"dji_tdscan",
            "Nasd100_tdscan":"nasd100_tdscan",
            "Optionable_tdscan":"optionable_tdscan",
            "PennyOptions_tdscan":"pennyincrementoptions_tdscan",
            "Russell_tdscan":"russell_tdscan",
            "Sp400_tdscan":"sp400_tdscan",
            "Sp500_tdscan":"sp500_tdscan",
            "WeeklyOptions_tdscan":"weeklyoptions_tdscan",
            "Movers":"tdmoversdata",
            "Sectors":"sectors_tdscan"
        },
        assetTypes={
            "Uni":"EQUITY",
            "Dji_tdscan":"EQUITY",
            "Nasd100_tdscan":"EQUITY",
            "Optionable_tdscan":"EQUITY",
            "PennyOptions_tdscan":"EQUITY",
            "Russell_tdscan":"EQUITY",
            "Sp400_tdscan":"EQUITY",
            "Sp500_tdscan":"EQUITY",
            "WeeklyOptions_tdscan":"EQUITY",
            "Movers":"EQUITY",
            "Sectors":"EQUITY"
        },iqHost=f'{IQ_LOCATION}',iqPort=int(IQ_PORT))
    if localDate not in holidays: 
        minBool = False
        dailyBool = False
        if len(minbeginDate) > 0:
            minBool = True
        if len(daybeginDate) > 0:
            dailyBool = True
        if auto:
            minBool = True
            dailyBool = True
        main.runIqRequests(minute=minBool,daily=dailyBool,minbeginDate=minbeginDate,daybeginDate=daybeginDate,fullMarket=fullMarket)
    else:
        main.log.info(str(localDate) + ' is a Holiday')
    main.exit()