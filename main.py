import statistics
from collections import defaultdict, OrderedDict
import time
import threading
import typing as t
from datetime import datetime, timedelta

from ibapi import wrapper
from ibapi.client import EClient

from ibapi.order import Order
from ibapi.contract import *
from ibapi.ticktype import *
from ibapi.common import BarData

CANDLES_IN_DAY = 78
CANDLE_TIME_IN_SECONDS = 300
TOTAL_SECONDS_TO_FETCH = (CANDLES_IN_DAY - 1) * CANDLE_TIME_IN_SECONDS
TIME_TO_PERFROM_CANDLE_ANALYSIS = 59580  # "16:35:00"
CONSTANT_QUANTITIY_TO_ORDER = 1
BASE_REQUEST_NUMBER = 0

DEMO_MODE = True
if DEMO_MODE:
    days_back = 1
else:
    days_back = 1


def generate_contract_for_symbol(symbol_name: str, exchange: str = 'SMART') -> Contract:
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
        return self.first_diff > 2 or (self.first_diff > 1 and self.first_value < 100)

    def is_last_candle_higher_than_4fat(self) -> bool:
        """
        checks if stock is above the 4 '4fat' conditions
        """
        return self.first_value - self.first_diff > self.max_value_of_4fat

    def is_eligible_to_purchase(self) -> bool:
        return self.is_last_candle_higher_than_4fat() and self.is_last_candle_big_enough()


def generate_request_index():
    globals()['BASE_REQUEST_NUMBER'] += 1
    return BASE_REQUEST_NUMBER


SYMBOLS = ['MU', 'AAPL', 'MSFT', 'JD', 'PDD', 'FSLY']

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
        print(f'[{reqId}] The current ask price is: {price}, ticktype: {TickType}, attrib:{attrib}')

    def historicalData(self, reqId: int, bar: BarData):
        # print(f'[{reqId}] Time: {bar.date} Close: {bar.close}')
        self.fetched_data[ID_TO_SYMBOL[reqId]][bar.date] = bar

    def openOrder(self, orderId: int, contract: Contract, order: Order, orderState: str):
        symbol_name = ORDER_IDS_TO_SYMBOL.get(orderId)
        print(f'open order for {symbol_name} has been performed. current order start - {orderState}')

    def orderStatus(self, orderId: int, status: str, filled: float, remaining: float, avgFillPrice: float,
                    permId: int, parentId: int, lastFillPrice: float, clientId: int, whyHeld: str, mktCapPrice: float):
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

                bottom_stop_value = symbol_object.first_max - symbol_object.first_diff
                sell_value = symbol_object.first_max + symbol_object.first_diff * 2
                self.sell_stop_for_stock(bottom_stop_value, CONSTANT_QUANTITIY_TO_ORDER, symbol_object, symbol_name)
                self.sell_stock(sell_value, CONSTANT_QUANTITIY_TO_ORDER, symbol_object, symbol_name)

            # HANLDLING FILLED 'SELL' ORDERS
            symbol_name = SELL_IDS_TO_SYMBOL.get(orderId)
            if symbol_name is not None:
                print(f'Order to sell was filled for {symbol_name}. buying price was {avgFillPrice}')
                symbol_name.is_owned = False
                symbol_name.selling_price = avgFillPrice
                symbol_name.selling_cap = mktCapPrice
                SELL_IDS_TO_SYMBOL.pop(orderId)

    def request_bars_for_stock(self, symbol_name: str, req_id: int, end_datetime: str = '',
                               amount_of_candles: int = 78):
        contract = generate_contract_for_symbol(symbol_name)
        self.reqHistoricalData(req_id, contract, end_datetime, f'{amount_of_candles * CANDLE_TIME_IN_SECONDS} S',
                               '5 mins',
                               'TRADES', 0, 1, False, [])

    def get_3_days_historical_data(self):
        DATES_TO_CONSIDER = get_formatted_end_datetimes()

        for req_id, symbol_name in ID_TO_SYMBOL.items():
            for end_datetime in DATES_TO_CONSIDER:
                self.request_bars_for_stock(symbol_name, req_id, end_datetime)
        time.sleep(10)

    def order_collected_historical_data(self):
        for symbol_name in SYMBOLS:
            self.fetched_data[symbol_name] = OrderedDict(sorted(self.fetched_data[symbol_name].items()))

    def buy_stop_for_stock(self, stop_price: int, quantity: int, contract: Contract, symbol_name: str):
        self.simplePlaceOid = self.nextOrderId()
        order = Order()
        order.action = "BUY"
        order.orderType = "LMT"
        order.auxPrice = stop_price
        order.totalQuantity = quantity
        ORDER_IDS_TO_SYMBOL[self.simplePlaceOid] = symbol_name
        self.placeOrder(self.simplePlaceOid, contract, order)

    def sell_stop_for_stock(self, stop_price: int, quantity: int, contract: Contract, symbol_name: str):
        self.simplePlaceOid = self.nextOrderId()
        order = Order()
        order.action = "SELL"
        order.orderType = "STP"
        order.auxPrice = stop_price
        order.totalQuantity = quantity
        SELL_IDS_TO_SYMBOL[self.simplePlaceOid] = symbol_name
        self.placeOrder(self.simplePlaceOid, contract, order)

    def sell_stock(self, stop_price: int, quantity: int, contract: Contract, symbol_name: str):
        self.simplePlaceOid = self.nextOrderId()
        order = Order()
        order.action = "SELL"
        order.orderType = "LMT"
        order.auxPrice = stop_price
        order.totalQuantity = quantity
        SELL_IDS_TO_SYMBOL[self.simplePlaceOid] = symbol_name
        self.placeOrder(self.simplePlaceOid, contract, order)


def run_loop():
    app.run()


def setup_app() -> IBapi:
    app = IBapi()
    app.connect("127.0.0.1", 7497, 1)
    time.sleep(1)
    app.reqMarketDataType(3)
    return app


def get_date_string_for_historical_data(day: str) -> str:
    return day + " 23:00:00"


def get_last_trading_dates(amount_of_days:int = 3):
    return [(datetime.now() - timedelta(days=i + days_back)).strftime('%Y%m%d')
                     for i in range(amount_of_days)]


def get_formatted_end_datetimes(amount_of_days: int = 3) -> t.List[str]:
    end_datetimes = get_last_trading_dates(amount_of_days)
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


def get_4fat_values_for_symbols():
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
    print('waiting for end of candle 1 of market day to perform final analysis')
    while (datetime.now() - datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    ).total_seconds() < TIME_TO_PERFROM_CANDLE_ANALYSIS + (candles_for_start_of_day - 1) * 300:
        time.sleep(0.1)


def update_symbols_current_data():
    for symbol_name in SYMBOLS:
        app.fetched_data[symbol_name] = OrderedDict(sorted(app.fetched_data[symbol_name].items()))
        last_candle = list(app.fetched_data[symbol_name].values())[-1]
        print(f'last data for {symbol_name} on time of ')
        symbol_objects[symbol_name].first_value = last_candle.open
        symbol_objects[symbol_name].first_volume = last_candle.volume
        symbol_objects[symbol_name].first_diff = last_candle.close - last_candle.open
        symbol_objects[symbol_name].first_max = last_candle.high


def get_first_candle_of_market_day_for_symbols():
    print('first candle finished, finding eligible stock names')
    for symbol_name, symbol_object in symbol_objects.items():
        end_datetime = (datetime.now() - timedelta(days=days_back - 1)).strftime('%Y%m%d 16:35:00')
        app.request_bars_for_stock(symbol_name, symbol_object.id, end_datetime=end_datetime, amount_of_candles=1)
    time.sleep(5)
    update_symbols_current_data()


app = setup_app()
api_thread = threading.Thread(target=run_loop, daemon=True)
api_thread.start()
time.sleep(0.1)

app.get_3_days_historical_data()
get_4fat_values_for_symbols()
wait_for_end_of_candle_for_new_market_day(1)
get_first_candle_of_market_day_for_symbols()

for symbol_name in SYMBOLS:
    symbol_object = symbol_objects[symbol_name]
    print('%%%%%%%%%%%%%')
    print(f'{symbol_name}')
    print(f'current value - {symbol_object.first_value}')
    print(f'4fat - {symbol_object.collected_4fat}')
    if symbol_object.is_eligible_to_purchase():
        print(f'{symbol_object} is good for buying. waiting for passage of candle 1 max value to buy.')
        symbol_name.intention_to_buy = True
        app.buy_stop_for_stock(symbol_object.first_max, CONSTANT_QUANTITIY_TO_ORDER, symbol_object.contract)

time.sleep(10)
time.sleep(5000)
app.disconnect()

# Get current value
# Create sell conditions
