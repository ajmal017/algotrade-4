import statistics
from collections import defaultdict, OrderedDict
import time
import threading
import typing as t
from datetime import datetime, timedelta
import argparse

import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay

from ibapi import wrapper
from ibapi.client import EClient
from ibapi.order import Order
from ibapi.contract import *
from ibapi.ticktype import *
from ibapi.common import BarData

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--days_back', type=int, default='1')
parser.add_argument('--trade_mode', type=bool, default=False)
parser.add_argument('--historical_mode', type=bool, default=True)
parser.add_argument('--demo_mode', type=bool, default=True)
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

SYMBOLS = ["BABA","MSFT"]
#SYMBOLS = ["AAPL"]


class WaitList(list):
    def __init__(self, max_len: int):
        self.max_len = max_len

    def append(self, object) -> None:
        while 1:
            if self.max_len <= len(self):
                time.sleep(0.05)
            else:
                super(WaitList, self).append(object)
                return


OPEN_HISTORICAL_REQUESTS_IDS = WaitList(50)
if DEMO_MODE:
    days_back = args.days_back
    CONNECTION_PORT = 7497
else:
    days_back = 1
    CONNECTION_PORT = 7496
DAY_OF_TRADE_ANALYSIS = (datetime.now() - timedelta(days=days_back - 1))
DAY_OF_TRADE_ANALYSIS_FORMATTED = DAY_OF_TRADE_ANALYSIS.strftime('%Y%m%d')


def generate_contract_for_symbol(symbol_name: str, exchange: str = 'SMART') -> Contract:
    """
    Create a TWS contract object for the provided symbol_name (for the nasdaq exchange, on USD currency)
    """
    contract = Contract()
    contract.symbol = symbol_name
    contract.secType = 'STK'
    contract.exchange = exchange
    contract.currency = 'USD'
    return contract


class Symbol:
    def __init__(self, name: str, id: int = None):
        self.name = name
        if id is not None:
            self.id = id
        else:
            self.id = generate_request_index()
        self.max_value_of_4fat = None
        self.first_value = None
        self.first_volume = None
        self.first_diff = None
        self.first_close = None
        self.first_max = None
        self.collected_4fat = None
        self.intention_to_buy = False
        self.is_owned = False
        self.buying_price = None
        self.buying_cap = None
        self.selling_price = None
        self.selling_cap = None
        self.contract = generate_contract_for_symbol(name)

    def is_last_candle_big_enough(self) -> bool:
        """
        checks if last candle is at least 2 dollars, and if not checks if stock is under 100 and candle is above 1.5
        """
        last_candle_big_enough = self.first_diff > 2 or (self.first_diff > 1.5 and self.first_value < 100)
        # print(f'last_candle_big_enough - {last_candle_big_enough} - {self.first_diff}')
        return last_candle_big_enough

    def is_last_candle_higher_than_4fat(self) -> bool:
        """
        checks if stock is above the 4 '4fat' conditions
        """
        higher_than_4fat = self.first_value > self.max_value_of_4fat
        # print(f'higher_than_4fat - {higher_than_4fat}')
        return higher_than_4fat

    def is_eligible_to_purchase(self) -> bool:
        """
        runs all tests on Symbol required to validate purchasing
        """
        return self.is_last_candle_higher_than_4fat() and self.is_last_candle_big_enough()

    def calc_stop_value(self) -> float:

        return symbol_object.first_close - symbol_object.first_diff

    def calc_market_sell_value(self) -> float:
        return symbol_object.first_close + symbol_object.first_diff * 2

    def calc_profit(self) -> t.Union[float, None]:
        """
        Calculates the profit ratio for the symbol if the symbol after buy + sell
        """
        if self.buying_cap is not None and self.selling_cap is not None:
            return self.selling_cap / self.buying_cap
        return None


def generate_request_index():
    """
    Creates a new request index to be used for a API request
    """
    globals()['BASE_REQUEST_NUMBER'] += 1
    return BASE_REQUEST_NUMBER


symbol_objects = {symbol_name: Symbol(symbol_name) for symbol_name in SYMBOLS}
ID_TO_SYMBOL = {symbol.id: symbol.name for symbol in symbol_objects.values()}
ORDER_IDS_TO_SYMBOL = {}
SELL_IDS_TO_SYMBOL = {}


class Wrapper(wrapper.EWrapper):
    pass


class Client(EClient):
    pass


class IBapi(Wrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.fetched_data = defaultdict(lambda: {})

    def tickPrice(self, reqId: int, tickType: int, price: int, attrib: str):
        """
        Built-in handle response for request_tick_price request
        """
        print(f'[{reqId}] The current ask price is: {price}, ticktype: {TickType}, attrib:{attrib}')

    def historicalData(self, reqId: int, bar: BarData):
        """
        Built-in handle response for request_historical_data request
        """
        # print(f'[{reqId}] Time: {bar.date} Close: {bar.close}')
        self.fetched_data[ID_TO_SYMBOL[reqId]][bar.date] = bar
        try:
            OPEN_HISTORICAL_REQUESTS_IDS.remove(reqId)
        except:
            pass

    def openOrder(self, orderId: int, contract: Contract, order: Order, orderState: str):
        """
        Built-in handle response for place_order request after it has been placed
        """
        symbol_name = ORDER_IDS_TO_SYMBOL.get(orderId)
        print(f'open order for {symbol_name} has been performed. current order start - {orderState}')

    def orderStatus(self, orderId: int, status: str, filled: float, remaining: float, avgFillPrice: float,
                    permId: int, parentId: int, lastFillPrice: float, clientId: int, whyHeld: str, mktCapPrice: float):
        """
        Built-in handle response for place_order request after the order status has been changed
        """
        if status == "Filled":
            # HANDLING FILLED 'BUY LMT' ORDERS
            symbol_name = ORDER_IDS_TO_SYMBOL.get(orderId)
            if symbol_name is not None:
                print(f'Order to buy was filled for {symbol_name}. buying price was {avgFillPrice}')
                symbol_object = symbol_objects[symbol_name]
                symbol_name.is_owned = True
                symbol_name.intention_to_buy = False
                symbol_name.buying_price = avgFillPrice
                symbol_name.buying_cap = mktCapPrice
                ORDER_IDS_TO_SYMBOL.pop(orderId)

                bottom_stop_value = symbol_object.calc_stop_value()
                sell_value = symbol_object.calc_market_sell_value()
                self.place_sell_stop(bottom_stop_value, CONSTANT_QUANTITIY_TO_ORDER, symbol_object, symbol_name)
                self.place_sell_limit(sell_value, CONSTANT_QUANTITIY_TO_ORDER, symbol_object, symbol_name)

            # HANLDLING FILLED 'SELL' ORDERS
            symbol_name = SELL_IDS_TO_SYMBOL.get(orderId)
            if symbol_name is not None:
                print(f'Order to sell was filled for {symbol_name}. buying price was {avgFillPrice}')
                symbol_name.is_owned = False
                symbol_name.selling_price = avgFillPrice
                symbol_name.selling_cap = mktCapPrice
                SELL_IDS_TO_SYMBOL.pop(orderId)

    def request_bars_for_stock(self, symbol_name: str, end_datetime: str = '', amount_of_candles: int = 78):
        """
        Request 5 minute candles for stock
        """
        req_id = generate_request_index()
        ID_TO_SYMBOL[req_id] = symbol_name
        OPEN_HISTORICAL_REQUESTS_IDS.append(req_id)
        contract = generate_contract_for_symbol(symbol_name)
        self.reqHistoricalData(req_id, contract, end_datetime, f'{amount_of_candles * CANDLE_TIME_IN_SECONDS} S',
                               '5 mins', 'TRADES', 0, 1, False, [])

    def get_days_historical_data(self, symbol_name: str, amount_of_days: int):
        """
        Fetches 3 business days of market data for all symbols
        """
        DATES_TO_CONSIDER = get_formatted_end_datetimes(amount_of_days)

        for idx, end_datetime in enumerate(DATES_TO_CONSIDER):
            self.request_bars_for_stock(symbol_name, end_datetime)
            if idx % 60 == 0 and idx>1:
                time.sleep(600)
        wait_for_no_open_historical_requests()

    def order_collected_historical_data(self):
        """
        Orders all fetched historical data chronologically - if not used, data isn't promised to be in chronological
        order
        """
        for symbol_name in SYMBOLS:
            self.fetched_data[symbol_name] = OrderedDict(sorted(self.fetched_data[symbol_name].items()))

    def place_buy_market(self, stop_price: int, quantity: int, contract: Contract, symbol_name: str):
        self.simplePlaceOid = self.nextOrderId()
        order = Order()
        order.action = "BUY"
        order.orderType = "MKT"
        order.auxPrice = stop_price
        order.totalQuantity = quantity
        ORDER_IDS_TO_SYMBOL[self.simplePlaceOid] = symbol_name
        self.placeOrder(self.simplePlaceOid, contract, order)

    def place_sell_stop(self, stop_price: int, quantity: int, contract: Contract, symbol_name: str):
        self.simplePlaceOid = self.nextOrderId()
        order = Order()
        order.action = "SELL"
        order.orderType = "STP"
        order.auxPrice = stop_price
        order.totalQuantity = quantity
        SELL_IDS_TO_SYMBOL[self.simplePlaceOid] = symbol_name
        self.placeOrder(self.simplePlaceOid, contract, order)

    def place_sell_limit(self, stop_price: int, quantity: int, contract: Contract, symbol_name: str):
        self.simplePlaceOid = self.nextOrderId()
        order = Order()
        order.action = "SELL"
        order.orderType = "LMT"
        order.auxPrice = stop_price
        order.totalQuantity = quantity
        SELL_IDS_TO_SYMBOL[self.simplePlaceOid] = symbol_name
        self.placeOrder(self.simplePlaceOid, contract, order)


def run_loop():
    """
    Runs the background process to recieve and handle API responses
    """
    app.run()


def setup_app() -> IBapi:
    """
    Connects and performs set-up of the app
    """
    app = IBapi()
    app.connect("127.0.0.1", CONNECTION_PORT, 2)
    time.sleep(1)
    app.reqMarketDataType(3)
    return app


def get_date_string_for_historical_data(day: str) -> str:
    """
    Formats day to end datetime string for end of market day
    """
    return day + " 23:00:00"


def get_last_trading_dates(amount_of_days: int = 3):
    """
    Gets last amount_of_days business days
    """
    us_business_day = CustomBusinessDay(calendar=USFederalHolidayCalendar())
    start_date = pd.to_datetime(datetime.now() - timedelta(days=days_back))
    last_trading_days = [((start_date - day * us_business_day).to_pydatetime()).strftime('%Y%m%d') for day in
                         range(amount_of_days)]
    # print(f'last trading days -> {last_trading_days}')

    return last_trading_days


def get_formatted_end_datetimes(amount_of_days: int = 3) -> t.List[str]:
    """
    Formats end datetime to the TWS API format
    """
    end_datetimes = get_last_trading_dates(amount_of_days)
    formatted_end_datetimes = [get_date_string_for_historical_data(end_datetime)
                               for end_datetime in end_datetimes]
    return formatted_end_datetimes


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


def get_4fat_values_for_symbols():
    """
    Collects 4FAT data for all symbols that were fetched on historical_data requests (And appear in app.fetched_data)
    """
    app.order_collected_historical_data()
    for symbol_name, candles in app.fetched_data.items():
        candles_values = list(candles.values())
        collected_4fat = {
            '200avg': analyze_for_200_avg(candles_values),
            '20avg': analyze_for_20_avg(candles_values),
            '6max': analyze_for_6_max_value(candles_values),
            'closing': analyze_for_closing_price(candles_values)
        }
        max_value_of_4fat = max(collected_4fat.values())
        symbol_objects[symbol_name].max_value_of_4fat = max_value_of_4fat
        symbol_objects[symbol_name].collected_4fat = collected_4fat


def wait_for_end_of_candle_for_new_market_day(candles_for_start_of_day: int = 1):
    """
    Waits until the end of the first candle of the day
    """
    # print('waiting for end of candle 1 of market day to perform final analysis')
    while (datetime.now() - datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    ).total_seconds() < TIME_TO_PERFROM_CANDLE_ANALYSIS + (candles_for_start_of_day - 1) * 300:
        time.sleep(0.3)


def update_symbols_current_data():
    """
    Update all the "Symbol.first_x" properties for all symbols, using the last historical data point of each symbol
    """
    for symbol_name in SYMBOLS:
        app.fetched_data[symbol_name] = OrderedDict(sorted(app.fetched_data[symbol_name].items()))
        last_candle = list(app.fetched_data[symbol_name].values())[-1]
        # print(f'last data for {symbol_name} on time of {last_candle.date}')
        symbol_objects[symbol_name].first_value = last_candle.close
        symbol_objects[symbol_name].first_volume = last_candle.volume
        symbol_objects[symbol_name].first_diff = last_candle.close - last_candle.open
        symbol_objects[symbol_name].first_max = last_candle.high
        symbol_objects[symbol_name].first_close = last_candle.close


def get_first_candle_of_market_day_for_symbols():
    """
    Used for trade mode
    Gets first candle of market day for all stocks
    """
    # print('first candle finished, finding eligible stock names')
    for symbol_name, symbol_object in symbol_objects.items():
        end_datetime = DAY_OF_TRADE_ANALYSIS.strftime('%Y%m%d 16:35:00')
        app.request_bars_for_stock(symbol_name, end_datetime=end_datetime, amount_of_candles=1)
    wait_for_no_open_historical_requests()
    update_symbols_current_data()


def get_entire_current_day_of_data():
    """
    Used for historical mode
    Gets current day's data for all stocks
    """
    for symbol_name, symbol_object in symbol_objects.items():
        end_datetime = DAY_OF_TRADE_ANALYSIS.strftime('%Y%m%d 18:00:00')
        app.request_bars_for_stock(symbol_name, end_datetime=end_datetime, amount_of_candles=18)
    wait_for_no_open_historical_requests()
    app.order_collected_historical_data()


def test_historically_for_outcome(symbol: Symbol) -> float:
    """
    returns the historical sell price that was achieved using the algorithm
    """
    min_outcome = symbol.calc_stop_value()
    max_outcome = symbol.calc_market_sell_value()
    # print(f'looking for first reach of either value - {symbol.calc_stop_value()} , {symbol.calc_limit_value()}')
    data_points_to_predict_from = [data_point for date_of_data, data_point in
                                   app.fetched_data[symbol.name].items() if
                                   DAY_OF_TRADE_ANALYSIS_FORMATTED in date_of_data]
    data_points_to_predict_from = data_points_to_predict_from[1:]

    for data_point in data_points_to_predict_from:
        min_value = min(data_point.close, data_point.high, data_point.low)
        if min_value < min_outcome:
            return min_outcome
        max_value = max(data_point.close, data_point.high, data_point.low)
        if max_value > max_outcome:
            return max_outcome
    return symbol.first_max


def wait_for_no_open_historical_requests():
    """
    Waits until all historical requests have been answered
    """
    while 1:
        if len(OPEN_HISTORICAL_REQUESTS_IDS) > 0:
            time.sleep(0.2)
        else:
            return


app = setup_app()
api_thread = threading.Thread(target=run_loop, daemon=True)
api_thread.start()
time.sleep(0.3)

for symbol_name in SYMBOLS:
    print(symbol_name)
    AMOUNT_OF_DAYS = 239
    app.get_days_historical_data(symbol_name, AMOUNT_OF_DAYS)
    list_of_data = [vars(bar) for bar in sorted(list(app.fetched_data[symbol_name].values()), key=lambda x: x.date)]
    csv_dataframe = pd.DataFrame.from_dict(list_of_data)
    # csv_dataframe.append(app.fetched_data[symbol_name])
    csv_dataframe.to_csv(f'{symbol_name}_{AMOUNT_OF_DAYS}_{DAY_OF_TRADE_ANALYSIS_FORMATTED}.csv', index=False)


app.disconnect()