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
parser.add_argument('--trade_mode', type=bool, default=True)
parser.add_argument('--historical_mode', type=bool, default=False)
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

SYMBOLS = ["AAPL", "MSFT", "FB", "BABA", "TSM", "V", "JPM", "JNJ", "WMT", "PG", "DIS", "HD"]


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
        self.min_value_of_4fat = None
        self.first_value = None
        self.first_volume = None
        self.first_open = None
        self.first_diff = None
        self.first_close = None
        self.first_max = None
        self.first_low = None
        self.collected_4fat = None
        self.intention_to_buy = False
        self.is_short_mode = False
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
        last_candle_big_enough = 5 > self.first_diff > 0.2

        wax_wire_ratio_is_good = self.first_diff < 2 * abs(self.first_close - self.first_open)
        # print(f'last_candle_big_enough - {last_candle_big_enough} - {self.first_diff}')
        return last_candle_big_enough and wax_wire_ratio_is_good

    def is_last_candle_higher_than_4fat(self) -> bool:
        """
        checks if stock is above the 4 '4fat' conditions
        """
        higher_than_4fat = self.first_value > self.max_value_of_4fat
        # print(f'higher_than_4fat - {higher_than_4fat}')
        return higher_than_4fat

    def is_first_candle_low_in_range(self) -> bool:
        """
        checks if lowest value of first candle is within 5 dollars range from the 20avg
        """
        return abs(self.collected_4fat['20avg'] - self.first_close) < 5

    def is_eligible_to_purchase(self) -> bool:
        """
        runs all tests on Symbol required to validate purchasing
        """
        return self.is_last_candle_higher_than_4fat() and self.is_last_candle_big_enough() \
               and self.is_first_candle_low_in_range()

    def is_eligible_to_short(self) -> bool:
        return self.first_value < self.min_value_of_4fat and self.is_last_candle_big_enough() \
               and self.is_first_candle_low_in_range()

    def calc_stop_value(self) -> float:

        return self.first_close - self.first_diff

    def calc_market_sell_value(self) -> float:
        if self.first_diff > 4:
            return self.first_close + self.first_diff * 2
        return self.first_close + self.first_diff * 2

    def short_stop_value(self) -> float:
        return self.first_close + self.first_diff

    def short_sell_value(self) -> float:
        if self.first_diff > 4:
            return self.first_close - self.first_diff
        return self.first_close - self.first_diff * 2

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


def generate_order_id():
    """
    Creates a new request index to be used for a API request
    """
    globals()['BASE_ORDER_ID'] += 1
    return BASE_ORDER_ID


symbol_objects = {symbol_name: Symbol(symbol_name) for symbol_name in SYMBOLS}
ID_TO_SYMBOL = {symbol.id: symbol.name for symbol in symbol_objects.values()}
ORDER_IDS_TO_SYMBOL = {}
SELL_IDS_TO_SYMBOL = {}


def cancel_all_standing_orders_for_stock(stock):
    print(f'closing all standing orders for {stock.name}')
    for order_id, v in ORDER_IDS_TO_SYMBOL.items():
        if v == stock.name:
            app.cancel_order(order_id)
            ORDER_IDS_TO_SYMBOL.pop(order_id, None)

    for order_id, v in SELL_IDS_TO_SYMBOL.items():
        if v == stock.name:
            app.cancel_order(order_id)
            SELL_IDS_TO_SYMBOL.pop(order_id, None)


class Wrapper(wrapper.EWrapper):
    pass


class Client(EClient):
    pass


class IBapi(Wrapper, EClient):
    nextorderId = None

    def __init__(self):
        EClient.__init__(self, self)
        self.fetched_data = defaultdict(lambda: {})

    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.nextorderId = orderId

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
        print(f'order [{orderId}] for {symbol_name} has been performed.')

    def orderStatus(self, orderId: int, status: str, filled: float, remaining: float, avgFillPrice: float,
                    permId: int, parentId: int, lastFillPrice: float, clientId: int, whyHeld: str, mktCapPrice: float):
        """
        Built-in handle response for place_order request after the order status has been changed
        """
        if status == "Filled":
            # HANDLING FILLED 'BUY' ORDERS
            symbol_name = ORDER_IDS_TO_SYMBOL.get(orderId)
            if symbol_name is not None:
                symbol = symbol_objects[symbol_name]
                symbol.is_owned = True
                symbol.intention_to_buy = False
                symbol.buying_price = avgFillPrice
                symbol.buying_cap = mktCapPrice
                ORDER_IDS_TO_SYMBOL.pop(orderId)

                bottom_stop_value = symbol.calc_stop_value()
                sell_value = symbol.calc_market_sell_value()

                time.sleep(0.5)
                if not symbol.is_short_mode:
                    print(f'Order [{orderId}] to buy was filled for {symbol_name}. buying price was {avgFillPrice}'
                          f'. selling for range - {bottom_stop_value}, {sell_value}')
                    self.place_order(bottom_stop_value, CONSTANT_QUANTITIY_TO_ORDER, symbol, "SELL", "STP")
                    self.place_order(sell_value, CONSTANT_QUANTITIY_TO_ORDER, symbol, "SELL", "LMT")
                    self.place_order(1000, CONSTANT_QUANTITIY_TO_ORDER, symbol, "SELL", "MOC")
                else:
                    cancel_all_standing_orders_for_stock(symbol)

            # HANDLING FILLED 'SELL' ORDERS
            symbol_name = SELL_IDS_TO_SYMBOL.get(orderId)
            if symbol_name is not None:
                print(f'Order [{orderId}] to sell was filled for {symbol_name}. selling price was {avgFillPrice}')
                symbol = symbol_objects[symbol_name]
                symbol.is_owned = False
                symbol.selling_price = avgFillPrice
                symbol.selling_cap = mktCapPrice
                SELL_IDS_TO_SYMBOL.pop(orderId)

                if symbol.is_short_mode:
                    self.place_order(symbol.short_stop_value(), CONSTANT_QUANTITIY_TO_ORDER, symbol, "BUY", "STP")
                    self.place_order(symbol.short_buy_value(), CONSTANT_QUANTITIY_TO_ORDER, symbol, "BUY", "LMT")
                    self.place_order(0, CONSTANT_QUANTITIY_TO_ORDER, symbol, "BUY", "MOC")
                else:
                    cancel_all_standing_orders_for_stock(symbol)

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

    def get_3_days_historical_data(self):
        """
        Fetches 3 business days of market data for all symbols
        """
        DATES_TO_CONSIDER = get_formatted_end_datetimes()

        for symbol_name in SYMBOLS:
            for end_datetime in DATES_TO_CONSIDER:
                self.request_bars_for_stock(symbol_name, end_datetime)
        wait_for_no_open_historical_requests()

    def order_collected_historical_data(self):
        """
        Orders all fetched historical data chronologically - if not used, data isn't promised to be in chronological
        order
        """
        for symbol_name in SYMBOLS:
            self.fetched_data[symbol_name] = OrderedDict(sorted(self.fetched_data[symbol_name].items()))

    def place_order(self, price: int, quantity: int, symbol: Symbol, action: str, order_type: str):
        order_id = generate_order_id()
        allowed_order_types = ["STP", "MKT", "LMT", "MOC"]
        if order_type not in allowed_order_types:
            raise ValueError(f'order_type not in {allowed_order_types}')
        if action == "SELL":
            SELL_IDS_TO_SYMBOL[order_id] = symbol.name
        elif action == "BUY":
            ORDER_IDS_TO_SYMBOL[order_id] = symbol.name
        else:
            raise ValueError(f'input action isnt one of ["BUY","SELL"]')
        order = self.create_order_object(action, order_type, price, quantity)
        self.placeOrder(order_id, symbol.contract, order)

    def create_order_object(self, action: str, order_type: str, price: float, quantity: float, gtd: str = None):
        order = Order()
        order.action = action
        order.orderType = order_type
        order.auxPrice = price
        order.lmtPrice = price
        order.totalQuantity = quantity
        if gtd is not None:
            order.goodTillDate = DAY_OF_TRADE_ANALYSIS_FORMATTED + f" 09:40:00 EST"
        return order

    def cancel_order(self, order_id: int):
        self.cancelOrder(order_id)


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
        min_value_of_4fat = min(collected_4fat.values())
        symbol_objects[symbol_name].max_value_of_4fat = max_value_of_4fat
        symbol_objects[symbol_name].min_value_of_4fat = min_value_of_4fat
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
        symbol_objects[symbol_name].first_open = last_candle.open
        symbol_objects[symbol_name].first_value = last_candle.close
        symbol_objects[symbol_name].first_volume = last_candle.volume
        symbol_objects[symbol_name].first_diff = last_candle.high - last_candle.low
        symbol_objects[symbol_name].first_max = last_candle.high
        symbol_objects[symbol_name].first_low = last_candle.low
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


def open_orders_condition():
    return any([symbol.is_owned for symbol in list(symbol_objects.values())]) or any(
        [symbol.intention_to_buy for symbol in list(symbol_objects.values())])


def wait_for_no_open_orders():
    while 1:
        if open_orders_condition():
            time.sleep(0.2)
        else:
            time.sleep(0.5)
            if not open_orders_condition():
                return


app = setup_app()
api_thread = threading.Thread(target=run_loop, daemon=True)
api_thread.start()
time.sleep(0.3)

BASE_ORDER_ID = app.nextorderId

app.get_3_days_historical_data()
get_4fat_values_for_symbols()

print(f'finished collecting and analyzing historical data, waiting for trading day to begin')

if TRADE_MODE:
    wait_for_end_of_candle_for_new_market_day(1)
get_first_candle_of_market_day_for_symbols()

for symbol_name in SYMBOLS:
    symbol_object = symbol_objects[symbol_name]
    print(f'{symbol_name} - current value - {symbol_object.first_value}')
    print(f'4fat - {symbol_object.collected_4fat}')

for symbol_name in SYMBOLS:
    symbol_object = symbol_objects[symbol_name]
    if symbol_object.is_eligible_to_purchase():
        print(f'{symbol_object.name} is good for buying.')
        print(f'{symbol_object.calc_market_sell_value()}, {symbol_object.calc_stop_value()}')
        symbol_object.intention_to_buy = True
        if TRADE_MODE:
            app.place_order(symbol_object.first_max, CONSTANT_QUANTITIY_TO_ORDER, symbol_object, "BUY", "LMT")
            time.sleep(2)  # Wait so that the order is finished processing and a new order can be made

    if symbol_object.is_eligible_to_short() and 1 == 0:
        print(f'{symbol_object.name} is good for short.')
        symbol_object.intention_to_buy = True
        symbol_object.is_short_mode = True
        if TRADE_MODE:
            app.place_order(symbol_object.first_low, CONSTANT_QUANTITIY_TO_ORDER, symbol_object, "SELL", "LMT")

print(f'validating that there are no open requests')
wait_for_no_open_orders()
print(f'all orders are completed, closing session')
import pdb

pdb.set_trace()
app.disconnect()
# https://filipmolcik.com/headless-ubuntu-server-for-ib-gatewaytws/
