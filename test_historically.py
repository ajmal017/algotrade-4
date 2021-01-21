import statistics
from collections import defaultdict, OrderedDict
import time
import threading
import typing as t
from datetime import datetime, timedelta
import argparse

import pandas as pd
from .main import *


parser = argparse.ArgumentParser(description='Process some integers.')
args = parser.parse_args()

TRADE_MODE = args.trade_mode
HISTORICAL_MODE = args.historical_mode
if TRADE_MODE == HISTORICAL_MODE:
    raise (ValueError("historical_mode and trade_mode can't both be true/false"))

DEMO_MODE = args.demo_mode

CANDLES_IN_DAY = 78
CANDLE_TIME_IN_SECONDS = 300
TOTAL_SECONDS_TO_FETCH = (CANDLES_IN_DAY - 1) * CANDLE_TIME_IN_SECONDS
TIME_TO_PERFROM_CANDLE_ANALYSIS = 59580  # "16:35:00"
CONSTANT_QUANTITIY_TO_ORDER = 1
BASE_REQUEST_NUMBER = 1000


SYMBOLS = ["AAPL", "MSFT", "FB", "BABA", "TSM", "V", "JPM", "JNJ", "WMT", "PG", "DIS", "HD"]# "PYPL", "INTC", "VZ",
           #"NKE", "NVS", "MRK", "TM", "CRM", "ABT", "PEP", "ABBV", "PDD", "ORCL", "BHP", "LLY", "CVX", "QCOM", "DHR"]
           # "ACN", "NVO", "NEE", "MDT", "TMUS", "UL", "MCD", "TXN", "SAP", "BMY", "BBL", "HON", "UNP", "AMGN", "UPS",
           # "HDB", "C", "JD", "LIN", "MS", "BUD", "AZN", "LOW", "SNE", "PM", "RY", "SBUX", "SE", "BA", "IBM", "SCHW",
           # "AMD", "TD", "SQ", "RTX", "CAT", "RIO", "UBER", "TGT", "CVS", "AXP", "AMT", "MMM", "AMAT", "DE",
           # "DEO", "EL", "SYK", "MU", "NIO", "BIDU", "MDLZ", "TJX", "FIS", "CI", "CNI", "GILD", "ZTS", "BDX", "BEKE",
           # "NTES", "GM", "FISV", "PLD", "CL", "CSX", "TFC", "CB", "ATVI"]

DAY_OF_TRADE_ANALYSIS = (datetime.now() - timedelta(days=days_back - 1))
DAY_OF_TRADE_ANALYSIS_FORMATTED = DAY_OF_TRADE_ANALYSIS.strftime('%Y%m%d')

symbol_objects = {symbol_name: Symbol(symbol_name) for symbol_name in SYMBOLS}

def load_historical_data_to_symbol_object(dataframe: pd.DataFrame, symbol_name: str):
    pass

def load_historical_data_from_csv_to_dataframe(file_name: str) -> pd.DataFrame:
    pass


for symbol in symbol_objects:
    historical_dataframe = load_historical_data_from_csv_to_dataframe(symbol.name + '.csv')
    for #ITERATE OVER 4 DAY CHUNKS FROM ^:
        load_historical_data_to_symbol_object(symbol_name, historical_dataframe)
        if symbol_object.is_eligible_to_purchase():
            if HISTORICAL_MODE:
                # print('testing historically')
                symbol_object.buying_price = symbol_object.first_max
                symbol_object.buying_cap = symbol_object.buying_price * CONSTANT_QUANTITIY_TO_ORDER
                symbol_object.selling_price = test_historically_for_outcome(symbol_object)
                symbol_object.selling_cap = symbol_object.selling_price * CONSTANT_QUANTITIY_TO_ORDER


# https://filipmolcik.com/headless-ubuntu-server-for-ib-gatewaytws/
