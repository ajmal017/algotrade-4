import statistics
import math
from collections import defaultdict, OrderedDict
import random
import time
import threading
import typing as t
from datetime import datetime, timedelta

from ibapi import wrapper
from ibapi.client import EClient

from ibapi.contract import *
from ibapi.ticktype import *
from ibapi.common import BarData

CANDLES_IN_DAY = 78
CANDLE_TIME_IN_SECONDS = 300
TOTAL_SECONDS_TO_FETCH = (CANDLES_IN_DAY - 1) * CANDLE_TIME_IN_SECONDS
TIME_TO_PERFROM_CANDLE_ANALYSIS = 59580  # "16:35:00"

BASE_REQUEST_NUMBER = 0
DEMO_MODE = True
if DEMO_MODE:
    days_back = 2
else:
    days_back = 1


class Symbol:
    def __init__(self, name: str, id: int = None):
        self.name = name
        if id is not None:
            self.id = id
        else:
            self.id = generate_request_index()
        self.max_value_of_4fat = None
        self.current_value = None
        self.current_volume = None
        self.current_diff = None

    def is_last_candle_big_enough(self) -> bool:
        """
        checks if last candle is at least 2 dollars, and if not checks if stock is under 100 and candle is above 1.5
        """
        return self.current_diff > 2 or (self.current_diff > 1 and self.current_value < 100)

    def is_last_candle_higher_than_4fat(self) -> bool:
        """
        checks if stock is above the 4 '4fat' conditions
        """
        return self.current_value - self.current_diff > max_value_of_4fat

    def is_eligible_to_purchase(self) -> bool:
        return self.is_last_candle_higher_than_4fat() and self.is_last_candle_big_enough()

    def buy_stock(self, amount: int):
        pass

    def insert_stop_on_stock(self, value: int):
        pass

    def sell_stock(self, amount: int):
        pass


def generate_request_index():
    globals()['BASE_REQUEST_NUMBER'] += 1
    return BASE_REQUEST_NUMBER


SYMBOLS = ['MU', 'AAPL', 'MSFT', 'JD', 'PDD', 'FSLY']

symbol_objects = {symbol_name: Symbol(symbol_name) for symbol_name in SYMBOLS}
ID_TO_SYMBOL = {symbol.id: symbol.name for symbol in symbol_objects.values()}


class Wrapper(wrapper.EWrapper):
    pass


class Client(EClient):
    pass


class IBapi(Wrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.fetched_data = defaultdict(lambda: {})

    def tickPrice(self, reqId: int, tickType: int, price: int, attrib: str):
        print(f'[{reqId}] The current ask price is: {price}, ticktype: {TickType}, attrib:{attrib}')

    def historicalData(self, reqId: int, bar: BarData):
        # print(f'[{reqId}] Time: {bar.date} Close: {bar.close}')
        self.fetched_data[ID_TO_SYMBOL[reqId]][bar.date] = bar


def run_loop():
    app.run()


def setup_app() -> IBapi:
    app = IBapi()
    app.connect("127.0.0.1", 7497, 1)

    api_thread = threading.Thread(target=run_loop, daemon=True)
    api_thread.start()

    time.sleep(1)

    app.reqMarketDataType(3)


def generate_contract_for_symbol(symbol_name: str, exchange: str = 'SMART') -> Contract:
    contract = Contract()
    contract.symbol = symbol_name
    contract.secType = 'STK'
    contract.exchange = exchange
    contract.currency = 'USD'
    return contract


def get_date_string_for_historical_data(day: str) -> str:
    return day + " 23:00:00"


def get_formatted_end_datetimes(amount_of_days: int = 3) -> t.List[str]:
    end_datetimes = [(datetime.now() - timedelta(days=i + days_back)).strftime('%Y%m%d')
                     for i in range(amount_of_days)]
    formatted_end_datetimes = [get_date_string_for_historical_data(end_datetime)
                               for end_datetime in end_datetimes]
    return formatted_end_datetimes


def analyze_for_200_avg(candles_values: t.List[BarData]) -> int:
    return statistics.mean([candle.close for candle in candles_values[-200:]])


def analyze_for_20_avg(candles_values: t.List[BarData]) -> int:
    return statistics.mean([candle.close for candle in candles_values[-20:]])


def analyze_for_6_max_value(candles_values: t.List[BarData]) -> int:
    return max([candle.close for candle in candles_values[-6:]])


def analyze_for_closing_price(candles_values: t.List[BarData]) -> int:
    return candles_values[-1].close


def request_bars_for_stock(symbol_name: str, req_id: int, end_datetime: str = '', amount_of_candles: int = 78):
    contract = generate_contract_for_symbol(symbol_name)
    app.reqHistoricalData(req_id, contract, end_datetime, f'{amount_of_candles * CANDLE_TIME_IN_SECONDS} S', '5 mins',
                          'TRADES', 0, 1, False, [])


def get_3_days_historical_data():
    DATES_TO_CONSIDER = get_formatted_end_datetimes()

    for req_id, symbol_name in ID_TO_SYMBOL.items():
        for end_datetime in DATES_TO_CONSIDER:
            request_bars_for_stock(symbol_name, req_id, end_datetime)
    time.sleep(10)


def order_collected_historical_data():
    for symbol_name in SYMBOLS:
        app.fetched_data[symbol_name] = OrderedDict(sorted(app.fetched_data[symbol_name].items()))


def get_4fat_values_for_symbols():
    order_collected_historical_data()
    for symbol_name, candles in app.fetched_data.items():
        candles_values = list(candles.values())
        values_to_consider = [analyze_for_200_avg(candles_values), analyze_for_20_avg(candles_values),
                              analyze_for_6_max_value(candles_values), analyze_for_closing_price(candles_values)]
        max_value_of_4fat = max(values_to_consider)
        symbol_objects[symbol_name].max_value_of_4fat = max_value_of_4fat


def wait_for_end_of_candle_1_for_new_market_day():
    print('waiting for end of candle 1 of market day to perform final analysis')
    while (datetime.now() - datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    ).total_seconds() < TIME_TO_PERFROM_CANDLE_ANALYSIS:
        time.sleep(0.1)


def update_symbols_current_data():
    for symbol_name in SYMBOLS:
        app.fetched_data[symbol_name] = OrderedDict(sorted(app.fetched_data[symbol_name].items()))
        last_candle = list(app.fetched_data[symbol_name].values())[-1]
        symbol_objects[symbol_name].current_value = last_candle.close
        symbol_objects[symbol_name].current_volume = last_candle.volume
        symbol_objects[symbol_name].current_diff = last_candle.close - last_candle.open


def get_first_candle_of_market_day_for_symbols():
    print('first candle finished, finding eligible stock names')
    for symbol_name, symbol_object in symbol_objects.items():
        end_datetime = (datetime.now() - timedelta(days=days_back - 1)).strftime('%Y%m%d 16:35:00')
        request_bars_for_stock(symbol_name, symbol_object.id, end_datetime=end_datetime, amount_of_candles=1)
    time.sleep(5)
    update_symbols_current_data()

app = setup_app()
get_3_days_historical_data()
get_4fat_values_for_symbols()
wait_for_end_of_candle_1_for_new_market_day()
get_first_candle_of_market_day_for_symbols()


for symbol_name in SYMBOLS:
    if symbol_objects[symbol_name].is_eligible_to_purchase():
        print(f'{symbol_objects[symbol_name]} is good for buying. performing purchase.')
        symbol_objects[symbol_name].buy()
        symbol_objects[symbol_name].set_stop()

time.sleep(5)
app.disconnect()

# Get current value
# Create sell conditions
