import statistics
from collections import defaultdict, OrderedDict
import time
import threading
import typing as t
from datetime import datetime, timedelta
import argparse

import pandas as pd
from main import *

CANDLES_IN_DAY = 78
CANDLE_TIME_IN_SECONDS = 300
TOTAL_SECONDS_TO_FETCH = (CANDLES_IN_DAY - 1) * CANDLE_TIME_IN_SECONDS
TIME_TO_PERFROM_CANDLE_ANALYSIS = 59580  # "16:35:00"
CONSTANT_QUANTITIY_TO_ORDER = 1
BASE_REQUEST_NUMBER = 1000


SYMBOLS = ["PTON"]# "PYPL", "INTC", "VZ",
           #"NKE", "NVS", "MRK", "TM", "CRM", "ABT", "PEP", "ABBV", "PDD", "ORCL", "BHP", "LLY", "CVX", "QCOM", "DHR"]
           # "ACN", "NVO", "NEE", "MDT", "TMUS", "UL", "MCD", "TXN", "SAP", "BMY", "BBL", "HON", "UNP", "AMGN", "UPS",
           # "HDB", "C", "JD", "LIN", "MS", "BUD", "AZN", "LOW", "SNE", "PM", "RY", "SBUX", "SE", "BA", "IBM", "SCHW",
           # "AMD", "TD", "SQ", "RTX", "CAT", "RIO", "UBER", "TGT", "CVS", "AXP", "AMT", "MMM", "AMAT", "DE",
           # "DEO", "EL", "SYK", "MU", "NIO", "BIDU", "MDLZ", "TJX", "FIS", "CI", "CNI", "GILD", "ZTS", "BDX", "BEKE",
           # "NTES", "GM", "FISV", "PLD", "CL", "CSX", "TFC", "CB", "ATVI"]

symbol_objects = {symbol_name: Symbol(symbol_name) for symbol_name in SYMBOLS}


def load_historical_data_to_symbol_object(symbol_name: str, dataframe: pd.DataFrame, first_candle):
    candles_values = [b for i, b in dataframe.iterrows()]
    symbol_objects[symbol_name].max_value_of_4fat = get_4fat_values_for_symbol(candles_values)
    first_candle_of_test_day = first_candle
    symbol_objects[symbol_name].first_value = first_candle_of_test_day.close
    symbol_objects[symbol_name].first_volume = first_candle_of_test_day.volume
    symbol_objects[symbol_name].first_diff = first_candle_of_test_day.close - first_candle_of_test_day.open
    symbol_objects[symbol_name].first_close = first_candle_of_test_day.close
    symbol_objects[symbol_name].first_max = first_candle_of_test_day.high


def get_4fat_values_for_symbol(candles_values):
    collected_4fat = {
        '200avg': analyze_for_200_avg(candles_values),
        '20avg': analyze_for_20_avg(candles_values),
        '6max': analyze_for_6_max_value(candles_values),
        'closing': analyze_for_closing_price(candles_values)
    }
    max_value_of_4fat = max(collected_4fat.values())
    return max_value_of_4fat



def analyze_for_200_avg(candles_values: t.List[BarData]) -> int:
    if len(candles_values) == 0:
        return 10 ** 10
    return statistics.mean([candle.close for candle in candles_values[-200:]])


def analyze_for_20_avg(candles_values: t.List[BarData]) -> int:
    if len(candles_values) == 0:
        return 10 ** 10
    return statistics.mean([candle.close for candle in candles_values[-20:]])


def analyze_for_6_max_value(candles_values: t.List[BarData]) -> int:
    if len(candles_values) == 0:
        return 10 ** 10
    return max([candle.close for candle in candles_values[-6:]])


def analyze_for_closing_price(candles_values: t.List[BarData]) -> int:
    if len(candles_values) == 0:
        return 10 ** 10
    return candles_values[-1].close


def load_historical_data_from_csv_to_dataframe(file_name: str) -> pd.DataFrame:
    return pd.read_csv(file_name)


def get_day_start_positions(historical_dataframe):
    positions = []
    current_val = 0
    for index, row in historical_dataframe.iterrows():
        if row.date[:8] != current_val:
            positions.append(index)
            current_val = row.date[:8]
    return positions


def test_historically_for_outcome(symbol, dataframe) -> float:
    """
    returns the historical sell price that was achieved using the algorithm
    """
    data_points_to_predict_from = [b for i, b in dataframe.iterrows()]
    min_outcome = symbol.calc_stop_value()
    max_outcome = symbol.calc_market_sell_value()

    for data_point in data_points_to_predict_from:
        min_value = min(data_point.close, data_point.high, data_point.low)
        if min_value < min_outcome:
            return min_outcome
        max_value = max(data_point.close, data_point.high, data_point.low)
        if max_value > max_outcome:
            return max_outcome
    return symbol.first_close


for symbol in symbol_objects.values():
    historical_dataframe = load_historical_data_from_csv_to_dataframe(symbol.name + '.csv')
    days_indexes = get_day_start_positions(historical_dataframe)
    results = []
    real_results = []
    for start_index, before_end_index, end_index in zip(days_indexes, days_indexes[2:], days_indexes[3:]):
        end_index = end_index-1
        first_candle_index = before_end_index + 1
        historical_outcome_dataframe = historical_dataframe[first_candle_index+1:end_index]
        first_candle = historical_dataframe.iloc[first_candle_index]
        historical_assesment_dataframe = historical_dataframe[start_index:before_end_index]
        load_historical_data_to_symbol_object(symbol.name, historical_assesment_dataframe, first_candle)

        # print(f'{symbol.max_value_of_4fat} - {symbol.first_value}')
        # print(f'{symbol.is_last_candle_higher_than_4fat()} ----- {symbol.first_diff}')

        if symbol.is_eligible_to_purchase():
            symbol.buying_price = symbol.first_close
            symbol.buying_cap = symbol.buying_price * CONSTANT_QUANTITIY_TO_ORDER
            symbol.selling_price = test_historically_for_outcome(symbol, historical_outcome_dataframe)
            symbol.selling_cap = symbol.selling_price * CONSTANT_QUANTITIY_TO_ORDER
            results.append(symbol.selling_price/symbol.buying_price)
            real_results.append((first_candle, symbol.selling_price/symbol.buying_price))
        else:
            results.append(1)
        symbol.result = results
    print(real_results)

# https://filipmolcik.com/headless-ubuntu-server-for-ib-gatewaytws/
