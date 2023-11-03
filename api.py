import logging
import datetime
import pytz
from os import environ
import json
from flask import Flask, request
from flask_apscheduler import APScheduler
from constants import PROJECT_ROOT, POSTGRES_LOCATION, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
from database import db
from DataBroker.priceHist import priceHist
# Custom Convert
from werkzeug.routing import PathConverter

class EverythingConverter(PathConverter):
    regex = '.*?'

# set configuration values
class Config:
    SCHEDULER_API_ENABLED = True

def create_app(db_location,debug=False):
    est = pytz.timezone('US/Eastern')
    if debug:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
            datefmt="%m-%d %H:%M",
            handlers=[logging.FileHandler(f'./logs/tdameritradeFlask_{datetime.datetime.now(tz=est).date()}.txt'), logging.StreamHandler()],
        )
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
            datefmt="%m-%d %H:%M",
            handlers=[logging.FileHandler(f'./logs/tdameritradeFlask_{datetime.datetime.now(tz=est).date()}.txt'), logging.StreamHandler()],
        )
    logger = logging.getLogger(__name__)
    app = Flask(__name__)
    app.config.from_object(Config())
    app.url_map.converters['everything'] = EverythingConverter
    app.config["SQLALCHEMY_DATABASE_URI"] = db_location
    if environ.get("FLASK_ENV") == "production":
        app.config["SERVER_NAME"] = "172.21.34.8:5000"
    db.init_app(app)
    # initialize scheduler
    scheduler = APScheduler()
    scheduler.init_app(app)
    params = {
            "host": f'{POSTGRES_LOCATION}',
            "port": f'{POSTGRES_PORT}',
            "database": f'{POSTGRES_DB}',
            "user": f'{POSTGRES_USER}',
            "password": f'{POSTGRES_PASSWORD}'
        }

    @scheduler.task('cron', id='price_hist_important', minute='5', hour='16', day_of_week='mon-thu', timezone='America/New_York')
    def runPriceHist_Important():
        return priceHist(auto=True)
    
    @scheduler.task('cron', id='price_hist_full', minute='5', hour='16', day_of_week='fri', timezone='America/New_York')
    def postRun_PriceHist():
        return priceHist(fullMarket=True,auto=True)

    @app.route('/run_pricehist', methods=['POST'])
    def postQuotes_Options():
        reminder_delay = int(request.args.get('delay',10))
        minStart = request.args.get('minStart',"")
        dayStart = request.args.get('dayStart',"")
        if len(request.args['fullMarket']) == 0:
            fullMarket = False
        else:
            fullMarket = json.loads(request.args['fullMarket'].lower())
        addPriceHist(scheduler, reminder_delay,minStart,dayStart,fullMarket)
        return json.dumps({
        'status':'success',
        'delay': reminder_delay
        })
    
    scheduler.start()
    return app

def addPriceHist(scheduler, delay,minStart="",dayStart="",fullMarket=False):
    logger = logging.getLogger(__name__)
    scheduled_time = datetime.datetime.now() + datetime.timedelta(seconds=delay)
    job_id = 'manual_price_hist'
    scheduler.add_job(id=job_id,func=priceHist, trigger='date',\
        run_date=scheduled_time,\
        kwargs={"minbeginDate":minStart,"daybeginDate":dayStart,"fullMarket":fullMarket})
    logger.info('IQ Price History Job Added')

app = create_app(f"postgresql://{PROJECT_ROOT}/{POSTGRES_DB}",False)