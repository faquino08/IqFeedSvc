from os import environ
import json

PROJECT_ROOT = environ['PROJECT_ROOT']
POSTGRES_LOCATION = environ['POSTGRES_LOCATION']
POSTGRES_PORT = environ['POSTGRES_PORT']
POSTGRES_DB = environ['POSTGRES_DB']
POSTGRES_USER = environ['POSTGRES_USER']
POSTGRES_PASSWORD = environ['POSTGRES_PASSWORD']
IQ_LOCATION = environ['IQ_LOCATION']
IQ_PORT = environ['IQ_PORT']
DEBUG = json.loads(environ['DEBUG_BOOL'].lower())
DEFAULT_MIN_START = environ.get('DEFAULT_MIN_START','20200101 073000')
DEFAULT_DAILY_START = environ.get('DEFAULT_DAILY_START','20180101')
APP_NAME = environ.get('APP_NAME','IqHistory')