# Imports
"""Import ans all settings unified."""
from datetime import datetime
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import random
import re
from shutil import copyfile
import sys
import mysql.connector
from mysql.connector import errorcode


errorcode.__name__
json.__name__
mysql.connector.__name__
os.__name__
sys.__name__
random.__name__
logging.__name__
RotatingFileHandler.__name__
datetime.__name__
copyfile.__defaults__
re.__name__


def log_init(_filename_, _conf_):
    """Main algorythm."""
    if not os.path.exists(APPS[_conf_]['PATH_LOG'][:-1]):
        os.makedirs(APPS[_conf_]['PATH_LOG'][:-1])
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s :: %(levelname)s :: %(message)s')
    file_handler = RotatingFileHandler(
        APPS[_conf_]['PATH_LOG'] + _filename_, 'a', 10000000, 1)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


# SYSTEM
APPS = {
    'default': {
        'PATH_LOG': './log/',
        'PATH_LIB': './lib/'
    }
}

# SQL Access
POSTGRE = {
    'default': {
        'HOST': 'localhost',
        'PORT': '5432',
        'USER': '',
        'PSW': '',
        'DB': ''
    }
}

MYSQL = {
    'default': {
        'HOST': 'localhost',
        'PORT': '3306',
        'USER': 'sysop',
        'PSW': '365280',
        'DB': 'COVIDLOAD'
    },
    'sleep': {
        'HOST': 'localhost',
        'PORT': '3306',
        'USER': 'sysop',
        'PSW': '365280',
        'DB': 'sleep'
    },
    'syno': {
        'HOST': '192.168.1.44',
        'PORT': '3307',
        'UNIX_SOCKET': 'run/mysqld/mysqld10.sock',
        'USER': 'sysop',
        'PSW': '@Pipoul@2019',
        'DB': 'COVIDLOAD'
    }
}

# Data type conversion
CONV_TYPE = {
    'psql': {
        'int': 'integer',
        'datetime': 'timestamp',
        'bool': 'boolean',
        'float': 'float8',
        'ObjectId': 'json',
        'str': 'text',
        None: 'json'
    }
}

MONGO = {
    'default': {
        'HOST': 'localhost',
        'PORT': '27017',
        'USER': '',
        'PSW': '',
        'DB': ''
    }
}

DATA = {
    'default': {
        'PATH': '/home/sysop/PycharmProjects/covid/data/'
    }
}
