# external modules
from ib_insync import ContFuture, Stock, Contract

# internal code/modules
from ibdatafetcher.ib_client import execute_fetch
from ibdatafetcher.spreads import (
    FutureCalendarSpread,
    ActionType,
    UnderlyingSymbol,
    SecType,
)


# equity_futures = ["/ES", "/NQ", "/RTY"]
# for symbol in equity_futures:
#     contract = ContFuture(symbol=symbol, exchange="GLOBEX")
#     execute_fetch([contract], ["TRADES"], 120)


popular_stocks = ["SPY", "QQQ", "TLT", "AAPL", "XLK"]
for symbol in popular_stocks:
    contract = Stock(symbol=symbol, exchange="SMART", currency="USD")
    execute_fetch([contract], ["TRADES"], 200)
