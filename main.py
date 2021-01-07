from collections import defaultdict
import time
import threading

from ibapi import wrapper
from ibapi.client import EClient

from ibapi.contract import *
from ibapi.ticktype import *

AMOUNT_OF_CANDLES_TO_CONSIDER = 4
CANDLE_TIME_IN_SECONDS = 300
TOTAL_SECONDS_TO_FETCH = (AMOUNT_OF_CANDLES_TO_CONSIDER-1) * CANDLE_TIME_IN_SECONDS


class Wrapper(wrapper.EWrapper):
    pass


class Client(EClient):
    pass


class IBapi(Wrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.fetched_data = defaultdict(lambda: {})

    def tickPrice(self, reqId, tickType, price, attrib):
        print(f'[{reqId}] The current ask price is: {price}, ticktype: {TickType}, attrib:{attrib}')

    def historicalData(self, reqId, bar):
        print(f'[{reqId}] Time: {bar.date} Close: {bar.close}')
        self.fetched_data[reqId][bar.date] = bar


def run_loop():
    app.run()


app = IBapi()
app.connect("127.0.0.1", 7497, 123)

api_thread = threading.Thread(target=run_loop, daemon=True)
api_thread.start()

time.sleep(1)


app.reqMarketDataType(3)
#Create contract object
eurusd_contract = Contract()
eurusd_contract.symbol = 'EUR'
eurusd_contract.secType = 'CASH'
eurusd_contract.exchange = 'IDEALPRO'
eurusd_contract.currency = 'USD'

#Request historical candles

# apple_contract = Contract()
# apple_contract.symbol = 'AAPL'
# apple_contract.secType = 'STK'
# apple_contract.exchange = 'SMART'
# apple_contract.currency = 'USD'
#
# google_contract = Contract()
# google_contract.symbol = 'GOOGL'
# google_contract.secType = 'STK'
# google_contract.exchange = 'SMART'
# google_contract.currency = 'USD'

SYMBOLS = ['MU', 'APPL', 'MSFT', 'JD', 'PDD', 'FSLY']


def generate_contract_for_symbol(symbol_name :str, exchange : str= 'SMART') -> Contract:
    contract = Contract()
    contract.symbol = symbol_name
    contract.secType = 'STK'
    contract.exchange = exchange
    contract.currency = 'USD'
    return contract

def get_end_of_exchange_date_time_by_day(day) -> str:
    return ''

def analyze_for_

for request_index, symbol_name in enumerate(SYMBOLS):
    contract = generate_contract_for_symbol(symbol_name)
    app.reqHistoricalData(request_index, eurusd_contract, '', f'{TOTAL_SECONDS_TO_FETCH} S', '5 mins', 'BID', 0, 1, False, [])
#a = time.time()
#app.reqMktData(1, apple_contract, '', False, False, [])
#app.reqMktData(2, google_contract, '', False, False, [])
#app.tickSnapshotEnd(1)

time.sleep(10)

time.sleep(5)
app.disconnect()
